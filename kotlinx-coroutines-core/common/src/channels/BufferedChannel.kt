package kotlinx.coroutines.channels

import kotlinx.atomicfu.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.ChannelResult.Companion.closed
import kotlinx.coroutines.channels.ChannelResult.Companion.failure
import kotlinx.coroutines.channels.ChannelResult.Companion.success
import kotlinx.coroutines.flow.internal.*
import kotlinx.coroutines.internal.*
import kotlinx.coroutines.selects.*
import kotlinx.coroutines.selects.TrySelectDetailedResult.*
import kotlin.contracts.*
import kotlin.coroutines.*
import kotlin.js.*
import kotlin.jvm.*
import kotlin.math.*
import kotlin.random.*
import kotlin.reflect.*

/**
The buffered channel implementation, which also serves as a rendezvous channel when the capacity is zero.
The high-level structure bases on a conceptually infinite array for storing elements and waiting requests,
separate counters of [send] and [receive] invocations that were ever performed, and an additional counter
that indicates the end of the logical buffer by counting the number of array cells it ever contained.
The key idea is that both [send] and [receive] start by incrementing their counters, assigning the array cell
referenced by the counter. In case of rendezvous channels, the operation either suspends and stores its continuation
in the cell or makes a rendezvous with the opposite request. Each cell can be processed by exactly one [send] and
one [receive]. As for buffered channels, [send]-s can also add elements without suspension if the logical buffer
contains the cell, while the [receive] operation updates the end of the buffer when its synchronization finishes.


Please see the ["Fast and Scalable Channels in Kotlin Coroutines"](https://arxiv.org/abs/2211.04986)
paper by Nikita Koval, Roman Elizarov, and Dan Alistarh for the detailed algorithm description.
 */
internal open class BufferedChannel<E>(
    /**
     * Channel capacity; `Channel.RENDEZVOUS` for rendezvous channel
     * and `Channel.UNLIMITED` for unlimited capacity.
     */
    private val capacity: Int,
    @JvmField
    internal val onUndeliveredElement: OnUndeliveredElement<E>? = null
) : Channel<E> {
    init {
        require(capacity >= 0) { "Invalid channel capacity: $capacity, should be >=0" }
        // NB: has second `init` after all the initialization with leaking `this`
    }

    /*
      The counters indicate the total numbers of send, receive, and buffer expansion calls
      ever performed. The counters are incremented in the beginning of the corresponding
      operation; thus, acquiring a unique (for the operation type) cell to process.
      The segments reference to the last working one for each operation type.

      Notably, the counter for send is combined with the channel closing status
      for synchronization simplicity and performance reasons.

      The logical end of the buffer is initialized with the channel capacity.
      When the channel is rendezvous or unlimited, the counter equals `BUFFER_END_RENDEZVOUS`
      or `BUFFER_END_RENDEZVOUS`, respectively, and never updates. The `bufferEndSegment`
      point to a special `NULL_SEGMENT` in this case.
     */
    private val sendersAndCloseStatus = atomic(0L)
    private val receivers = atomic(0L)
    private val bufferEnd = atomic(initialBufferEnd(capacity))

    /*
      Additionally to the counters above, we need an extra one that
      tracks the number of cells processed by `expandBuffer()`.
      When a receiver aborts, the corresponding cell might be
      physically removed from the data structure to avoid memory
      leaks, while it still can be unprocessed by `expandBuffer()`.
      In this case, `expandBuffer()` cannot know whether the
      removed cell contained sender or receiver and, therefore,
      cannot proceed. To solve the race, we ensure that cells
      correspond to cancelled receivers cannot be physically
      removed until the cell is processed.
      This additional counter enables the synchronization,
     */
    private val completedExpandBuffers = atomic(bufferEnd.value)

    private val isRendezvousOrUnlimited
        get() = bufferEnd.value.let { it == BUFFER_END_RENDEZVOUS || it == BUFFER_END_UNLIMITED }

    private val sendSegment: AtomicRef<ChannelSegment<E>>
    private val receiveSegment: AtomicRef<ChannelSegment<E>>
    private val bufferEndSegment: AtomicRef<ChannelSegment<E>>

    init {
        @Suppress("LeakingThis")
        val firstSegment = ChannelSegment(id = 0, prev = null, channel = this, pointers = 3)
        sendSegment = atomic(firstSegment)
        receiveSegment = atomic(firstSegment)
        // If this channel is rendezvous or has unlimited capacity, the algorithm never
        // invokes the buffer expansion procedure, and the corresponding segment reference
        // points to a special `NULL_SEGMENT` one and never updates.
        @Suppress("UNCHECKED_CAST")
        bufferEndSegment = atomic(if (isRendezvousOrUnlimited) (NULL_SEGMENT as ChannelSegment<E>) else firstSegment)
    }

    // #########################
    // ## The send operations ##
    // #########################

    override suspend fun send(element: E): Unit =
        sendImpl( // <-- this is an inline function
            element = element,
            // Do not create a continuation until it is required;
            // it is created later via [onNoWaiterSuspend], if needed.
            waiter = null,
            // Finish immediately if a rendezvous happens
            // or the element has been buffered.
            onRendezvousOrBuffered = {},
            // As no waiter is provided, suspension is impossible.
            onSuspend = { _, _ -> assert { false } },
            // According to the `send(e)` contract, we need to call
            // `onUndeliveredElement(..)` handler and throw an exception
            // if the channel is already closed.
            onClosed = { onClosedSend(element, coroutineContext) },
            // When `send(e)` decides to suspend, the corresponding
            // `onNoWaiterSuspend` function that creates a continuation
            // is called. The tail-call optimization is applied here.
            onNoWaiterSuspend = { segm, i, elem, s -> sendOnNoWaiterSuspend(segm, i, elem, s) }
        )

    private fun onClosedSend(element: E, coroutineContext: CoroutineContext) {
        onUndeliveredElement?.callUndeliveredElement(element, coroutineContext)
        throw recoverStackTrace(sendException)
    }

    private suspend fun sendOnNoWaiterSuspend(
        /* The working cell is specified by
        the segment and the index in it. */
        segment: ChannelSegment<E>,
        index: Int,
        /** The element to be inserted. */
        element: E,
        /** The global index of the cell. */
        s: Long
    ) = suspendCancellableCoroutineReusable sc@{ cont ->
        sendImplOnNoWaiter( // <-- this is an inline function
            segment = segment, index = index, element = element, s = s,
            // Store the created continuation as a waiter.
            waiter = cont,
            // If a rendezvous happens or the element has been buffered,
            // resume the continuation and finish. In case of prompt
            // cancellation, it is guaranteed that the element
            // has been already buffered or passed to receiver.
            onRendezvousOrBuffered = { cont.resume(Unit) },
            // Clean the cell on suspension and invoke
            // `onUndeliveredElement(..)` if needed.
            onSuspend = { segm, i -> cont.prepareSenderForSuspension(segm, i) },
            // If the channel is closed, call `onUndeliveredElement(..)` and complete the
            // continuation with the corresponding exception.
            onClosed = { onClosedSendOnNoWaiterSuspend(element, cont) },
        )
    }

    private fun CancellableContinuation<*>.prepareSenderForSuspension(
        /* The working cell is specified by
        the segment and the index in it. */
        segment: ChannelSegment<E>,
        index: Int
    ) {
        if (onUndeliveredElement == null) {
            // TODO: replace with a more efficient cancellation
            // TODO: mechanism  for segments when #3084 is finished.
            invokeOnCancellation(SenderOrReceiverCancellationHandler(segment, index).asHandler)
        } else {
            invokeOnCancellation(SenderWithOnUndeliveredElementCancellationHandler(segment, index, context).asHandler)
        }
    }

    private inner class SenderOrReceiverCancellationHandler(
        private val segment: ChannelSegment<E>,
        private val index: Int
    ) : BeforeResumeCancelHandler(), DisposableHandle {
        override fun dispose() {
            segment.onCancellation(index)
        }
        override fun invoke(cause: Throwable?) = dispose()
    }

    private inner class SenderWithOnUndeliveredElementCancellationHandler(
        private val segment: ChannelSegment<E>,
        private val index: Int,
        private val context: CoroutineContext
    ) : BeforeResumeCancelHandler(), DisposableHandle {
        override fun dispose() {
            segment.onSenderCancellationWithOnUndeliveredElement(index, context)
        }
        override fun invoke(cause: Throwable?) = dispose()
    }

    private fun onClosedSendOnNoWaiterSuspend(element: E, cont: CancellableContinuation<Unit>) {
        onUndeliveredElement?.callUndeliveredElement(element, cont.context)
        cont.resumeWithException(recoverStackTrace(sendException, cont))
    }

    override fun trySend(element: E): ChannelResult<Unit> {
        // Do not try to send the element if the plain `send(e)` operation would suspend.
        if (shouldSendSuspend(sendersAndCloseStatus.value)) return failure()
        // This channel either has waiting receivers or is closed.
        // Let's try to send the element!
        // The logic is similar to the plain `send(e)` operation, with
        // the only difference that we install `INTERRUPTED_SEND` in case
        // the operation decides to suspend.
        return sendImpl( // <-- this is an inline function
            element = element,
            // Store an already interrupted sender in case of suspension.
            waiter = INTERRUPTED_SEND,
            // Finish successfully when a rendezvous happens
            // or the element has been buffered.
            onRendezvousOrBuffered = { success(Unit) },
            // On suspension, the `INTERRUPTED_SEND` token has been installed,
            // and this `trySend(e)` fails. According to the contract,
            // we do not need to call [onUndeliveredElement] handler.
            onSuspend = { segm, _ ->
                segm.onSlotCleaned()
                failure()
            },
            // If the channel is closed, return the corresponding result.
            onClosed = { closed(sendException) }
        )
    }

    /**
     * This is a special `send(e)` implementation that returns `true` if the element
     * has been successfully sent, and `false` if the channel is closed.
     *
     * In case of coroutine cancellation, the element may be undelivered --
     * the [onUndeliveredElement] feature is unsupported in this implementation.
     *
     * Note that this implementation always invokes [suspendCancellableCoroutineReusable],
     * as we do not care about broadcasts performance -- they are already deprecated.
     */
    internal open suspend fun sendBroadcast(element: E): Boolean = suspendCancellableCoroutineReusable { cont ->
        check(onUndeliveredElement == null) {
            "the `onUndeliveredElement` feature is unsupported for `sendBroadcast(e)`"
        }
        sendImpl(
            element = element,
            waiter = SendBroadcast(cont),
            onRendezvousOrBuffered = { cont.resume(true) },
            onSuspend = { segm, i -> cont.prepareSenderForSuspension(segm, i) },
            onClosed = { cont.resume(false) }
        )
    }

    /**
     * Specifies waiting [sendBroadcast] operation.
     */
    private class SendBroadcast(val cont: CancellableContinuation<Boolean>) : Waiter

    /**
     * Abstract send implementation.
     */
    private inline fun <R> sendImpl(
        /* The element to be sent. */
        element: E,
        /* The waiter to be stored in case of suspension,
        or `null` if the waiter is not created yet.
        In the latter case, when the algorithm decides
        to suspend, [onNoWaiterSuspend] is called. */
        waiter: Any?,
        /* This lambda is invoked when the element has been
        buffered or a rendezvous with a receiver happens. */
        onRendezvousOrBuffered: () -> R,
        /* This lambda is called when the operation suspends in the
        cell specified by the segment and the index in it. */
        onSuspend: (segm: ChannelSegment<E>, i: Int) -> R,
        /* This lambda is called when the channel
        is observed in the closed state. */
        onClosed: () -> R,
        /* This lambda is called when the operation decides
        to suspend, but the waiter is not provided (equals `null`).
        It should create a waiter and delegate to `sendImplOnNoWaiter`. */
        onNoWaiterSuspend: (
            segm: ChannelSegment<E>,
            i: Int,
            element: E,
            s: Long
        ) -> R = { _, _, _, _ -> error("unexpected") }
    ): R {
        // Read the segment reference before the counter increment;
        // it is crucial to be able to find the required segment later.
        var segment = sendSegment.value
        while (true) {
            // Atomically increment the `senders` counter and obtain the
            // value right before the increment along with the close status.
            val sendersAndCloseStatusCur = sendersAndCloseStatus.getAndIncrement()
            val s = sendersAndCloseStatusCur.counter
            // Is this channel already closed? Keep the information.
            val closed = sendersAndCloseStatusCur.isClosedForSend0
            // Count the required segment id and the cell index in it.
            val id = s / SEGMENT_SIZE
            val i = (s % SEGMENT_SIZE).toInt()
            // Try to find the required segment if the initially obtained
            // one (in the beginning of this function) has lower id.
            if (segment.id != id) {
                // Find the required segment.
                segment = findSegmentSend(id, segment, closed) ?:
                    // The required segment has not been found.
                    // Finish immediately if this channel is closed,
                    // restarting the operation otherwise.
                    // In the latter case, the required segment was full
                    // of interrupted waiters and, therefore, removed
                    // physically to avoid memory leaks.
                    if (closed) {
                        return onClosed()
                    } else continue
            }
            // Update the cell according to the algorithm. Importantly, when
            // the channel is already closed, storing a waiter is illegal, so
            // the algorithm stores the `INTERRUPTED_SEND` token in this case.
            when(updateCellSend(segment, i, element, s, waiter, closed)) {
                RESULT_RENDEZVOUS -> {
                    // A rendezvous with a receiver has happened.
                    // The previous segments are no longer needed
                    // for the upcoming requests, so the algorithm
                    // resets the link to the previous segment.
                    segment.cleanPrev()
                    return onRendezvousOrBuffered()
                }
                RESULT_BUFFERED -> {
                    // The element has been buffered.
                    return onRendezvousOrBuffered()
                }
                RESULT_SUSPEND -> {
                    // The operation has decided to suspend and installed the
                    // specified waiter. If the channel was already closed,
                    // and the `INTERRUPTED_SEND` token has been installed as a waiter,
                    // this request finishes with the `onClosed()` action.
                    if (closed) {
                        segment.onSlotCleaned()
                        return onClosed()
                    }
                    return onSuspend(segment, i)
                }
                RESULT_CLOSED -> {
                    // This channel is closed.
                    // In case this segment is already or going to be
                    // processed by a receiver, ensure that all the
                    // previous segments are unreachable.
                    if (s < receiversCounter) segment.cleanPrev()
                    return onClosed()
                }
                RESULT_FAILED -> {
                    // Either the cell stores an interrupted receiver,
                    // or it was poisoned by a concurrent receiver.
                    // In both cases, all the previous segments are already processed,
                    segment.cleanPrev()
                    continue
                }
                RESULT_SUSPEND_NO_WAITER -> {
                    // The operation has decided to suspend,
                    // but no waiter has been provided.
                    return onNoWaiterSuspend(segment, i, element, s)
                }
            }
        }
    }

    private inline fun <R> sendImplOnNoWaiter(
        /* The working cell is specified by
        the segment and the index in it. */
        segment: ChannelSegment<E>,
        index: Int,
        /* The element to be sent. */
        element: E,
        /* The global index of the cell. */
        s: Long,
        /* The waiter to be stored in case of suspension. */
        waiter: Any,
        /* This lambda is invoked when the element has been
        buffered or a rendezvous with a receiver happens.*/
        onRendezvousOrBuffered: () -> R,
        /* This lambda is called when the operation suspends in the
        cell specified by the segment and the index in it. */
        onSuspend: (segm: ChannelSegment<E>, i: Int) -> R,
        /* This lambda is called when the channel
        is observed in the closed state. */
        onClosed: () -> R,
    ): R =
        // Update the cell again, now with the non-null waiter,
        // restarting the operation from the beginning on failure.
        // Check the `sendImpl(..)` function for the comments.
        when(updateCellSend(segment, index, element, s, waiter, false)) {
            RESULT_RENDEZVOUS -> {
                segment.cleanPrev()
                onRendezvousOrBuffered()
            }
            RESULT_BUFFERED -> {
                onRendezvousOrBuffered()
            }
            RESULT_SUSPEND -> {
                onSuspend(segment, index)
            }
            RESULT_CLOSED -> {
                if (s < receiversCounter) segment.cleanPrev()
                onClosed()
            }
            RESULT_FAILED -> {
                segment.cleanPrev()
                sendImpl(
                    element = element,
                    waiter = waiter,
                    onRendezvousOrBuffered = onRendezvousOrBuffered,
                    onSuspend = onSuspend,
                    onClosed = onClosed,
                )
            }
            else -> error("unexpected")
        }

    /**
     * Updates the working cell of an abstract send operation.
     */
    private fun updateCellSend(
        /* The working cell is specified by
        the segment and the index in it. */
        segment: ChannelSegment<E>,
        index: Int,
        /* The element to be sent. */
        element: E,
        /* The global index of the cell. */
        s: Long,
        /* The waiter to be stored in case of suspension. */
        waiter: Any?,
        closed: Boolean
    ): Int {
        // First, the algorithm stores the element,
        // performing the synchronization after that.
        // This way, receivers safely retrieve the
        // element, following the safe publication pattern.
        segment.storeElement(index, element)
        // Then, the cell state should be updated according to
        // its state machine; see the paper mentioned in the very
        // beginning for the cell life-cycle and the algorithm details.
        while (true) {
            // Read the current cell state.
            val state = segment.getState(index)
            when {
                // The cell is empty.
                state === null -> {
                    // If the element should be buffered, or a rendezvous should happen
                    // while the receiver is still coming, try to buffer the element.
                    // Otherwise, try to store the specified waiter in the cell.
                    if (bufferOrRendezvousSend(s) && !closed) {
                        // Move the cell state to `BUFFERED`.
                        if (segment.casState(index, null, BUFFERED)) {
                            // The element has been successfully buffered, finish.
                            return RESULT_BUFFERED
                        }
                    } else {
                        // This `send(e)` operation should suspend.
                        // However, in case the channel has already
                        // been observed closed, `INTERRUPTED_SEND`
                        // is installed instead.
                        when {
                            // The channel is closed
                            closed -> if (segment.casState(index, null, INTERRUPTED_SEND)) {
                                segment.onCancelledRequest(index, false)
                                return RESULT_CLOSED
                            }
                            // The waiter is not specified; return the corresponding result.
                            waiter == null -> return RESULT_SUSPEND_NO_WAITER
                            // Try to install the waiter.
                            else -> if (segment.casState(index, null, waiter)) return RESULT_SUSPEND
                        }
                    }
                }
                // This cell is in the logical buffer.
                state === IN_BUFFER -> {
                    // Try to buffer the element.
                    if (segment.casState(index, state, BUFFERED)) {
                        // The element has been successfully buffered, finish.
                        return RESULT_BUFFERED
                    }
                }
                // The cell stores a cancelled receiver.
                state === INTERRUPTED_RCV -> {
                    // Clean the element slot to avoid memory leaks and finish.
                    segment.cleanElement(index)
                    return RESULT_FAILED
                }
                // The cell is poisoned by a concurrent receive.
                state === POISONED -> {
                    // Clean the element slot to avoid memory leaks and finish.
                    segment.cleanElement(index)
                    return RESULT_FAILED
                }
                // The channel is already closed.
                state === CHANNEL_CLOSED -> {
                    // Clean the element slot to avoid memory leaks,
                    // ensure that the closing/cancellation procedure
                    // has been completed, and finish.
                    segment.cleanElement(index)
                    completeCloseOrCancel()
                    return RESULT_CLOSED
                }
                // A waiting receiver is stored in the cell.
                else -> {
                    assert { state is Waiter || state is WaiterEB }
                    // As the element will be passed directly to the waiter,
                    // the algorithm cleans the element slot in the cell.
                    segment.cleanElement(index)
                    // Unwrap the waiting receiver from `WaiterEB` if needed.
                    // As a receiver is stored in the cell, the buffer expansion
                    // procedure would finish, so senders simply ignore the "EB" marker.
                    val receiver = if (state is WaiterEB) state.waiter else state
                    // Try to make a rendezvous with the suspended receiver.
                    return if (receiver.tryResumeReceiver(element)) {
                        // Rendezvous! Move the cell state to `DONE_RCV` and finish.
                        segment.setState(index, DONE_RCV)
                        onReceiveDequeued()
                        RESULT_RENDEZVOUS
                    } else {
                        // The resumption has failed. Update the cell state correspondingly
                        // and clean the element field. It is also possible for a concurrent
                        // `expandBuffer()` or the cancellation handler to update the cell state;
                        // we can safely ignore these updates as senders dot help `expandBuffer()`.
                        if (segment.getAndSetState(index, INTERRUPTED_RCV) !== INTERRUPTED_RCV) {
                            segment.onCancelledRequest(index, true)
                        }
                        RESULT_FAILED
                    }
                }
            }
        }
    }

    /**
     * Checks whether a [send] invocation is bound to suspend if it is called
     * with the specified [sendersAndCloseStatus], [receivers], and [bufferEnd]
     * values. When this channel is already closed, the function returns `false`.
     *
     * Specifically, [send] suspends if the channel is not unlimited,
     * the number of receivers is greater than then index of the working cell of the
     * potential [send] invocation, and the buffer does not cover this cell
     * in case of buffered channel.
     * When the channel is already closed, [send] does not suspend.
     */
    @JsName("shouldSendSuspend0")
    private fun shouldSendSuspend(curSendersAndCloseStatus: Long): Boolean {
        // Does not suspend if the channel is already closed.
        if (curSendersAndCloseStatus.isClosedForSend0) return false
        // Does not suspend if a rendezvous may happen or the buffer is not full.
        return !bufferOrRendezvousSend(curSendersAndCloseStatus.counter)
    }

    /**
     * Returns `true` when the specified [send] should place
     * its element to the working cell without suspension.
     */
    private fun bufferOrRendezvousSend(curSenders: Long): Boolean =
        curSenders < bufferEnd.value || curSenders < receivers.value + capacity

    /**
     * Checks whether a [send] invocation is bound to suspend if it is called
     * with the current counter and close status values. See [shouldSendSuspend] for details.
     *
     * Note that this implementation is _false positive_ in case of rendezvous channels,
     * so it can return `false` when a [send] invocation is bound to suspend. Specifically,
     * the counter of `receive()` operations may indicate that there is a waiting receiver,
     * while it has already been cancelled, so the potential rendezvous is bound to fail.
     */
    internal open fun shouldSendSuspend(): Boolean = shouldSendSuspend(sendersAndCloseStatus.value)

    /**
     * Tries to resume this receiver with the specified [element] as a result.
     * Returns `true` on success and `false` otherwise.
     */
    @Suppress("UNCHECKED_CAST")
    private fun Any.tryResumeReceiver(element: E): Boolean = when(this) {
        is SelectInstance<*> -> { // `onReceiveXXX` select clause
            trySelect(this@BufferedChannel, element)
        }
        is ReceiveCatching<*> -> {
            this as ReceiveCatching<E>
            cont.tryResume0(success(element), onUndeliveredElement?.bindCancellationFun(element, cont.context))
        }
        is BufferedChannel<*>.BufferedChannelIterator -> {
            this as BufferedChannel<E>.BufferedChannelIterator
            tryResumeHasNext(element)
        }
        is CancellableContinuation<*> -> { // `receive()`
            this as CancellableContinuation<E>
            tryResume0(element, onUndeliveredElement?.bindCancellationFun(element, context))
        }
        else -> error("Unexpected receiver type: $this")
    }

    // ##########################
    // # The receive operations #
    // ##########################

    /**
     * This function is invoked when a receiver is added as a waiter in this channel.
     */
    protected open fun onReceiveEnqueued() {}

    /**
     * This function is invoked when a waiting receiver is no longer stored in this channel;
     * independently on whether it is caused by rendezvous, cancellation, or channel closing.
     */
    protected open fun onReceiveDequeued() {}

    /**
     * This function is invoked when the receiving operation ([receive], [tryReceive],
     * [BufferedChannelIterator.hasNext], etc.) finishes its synchronization -- either
     * completing due to an element retrieving or discovering this channel in the closed state,
     * or deciding to suspend if this channel is empty and not closed.
     *
     * We use this function to protect all receive operations with global lock in [ConflatedBroadcastChannel],
     * by acquiring the lock in the beginning of each operation and releasing it when the synchronization
     * completes, via this function.
     */
    protected open fun onReceiveSynchronizationCompleted() {}

    override suspend fun receive(): E =
        receiveImpl( // <-- this is an inline function
            // Do not create a continuation until it is required;
            // it is created later via [onNoWaiterSuspend], if needed.
            waiter = null,
            // Return the received element on successful retrieval from
            // the buffer or rendezvous with a suspended sender.
            // Also, inform the `BufferedChannel` extensions that
            // the synchronization of this receive operation is completed.
            onElementRetrieved = { element ->
                onReceiveSynchronizationCompleted()
                return element
            },
            // As no waiter is provided, suspension is impossible.
            onSuspend = { _, _, _ -> error("unexpected") },
            // Throw an exception if the channel is already closed.
            onClosed = { onClosedReceive() },
            // If `receive()` decides to suspend, the corresponding
            // `suspend` function that creates a continuation is called.
            // The tail-call optimization is applied here.
            onNoWaiterSuspend = { segm, i, r -> receiveOnNoWaiterSuspend(segm, i, r) }
        )

    private fun onClosedReceive(): E =
        throw recoverStackTrace(receiveException)
            .also { onReceiveSynchronizationCompleted() }

    private suspend fun receiveOnNoWaiterSuspend(
        /* The working cell is specified by
        the segment and the index in it. */
        segment: ChannelSegment<E>,
        index: Int,
        /* The global index of the cell. */
        r: Long
    ) = suspendCancellableCoroutineReusable { cont ->
        receiveImplOnNoWaiter( // <-- this is an inline function
            segment = segment, index = index, r = r,
            // Store the created continuation as a waiter.
            waiter = cont,
            // In case of successful element retrieval, resume
            // the continuation with the element and inform the
            // `BufferedChannel` extensions that the synchronization
            // is completed. Importantly, the receiver coroutine
            // may be cancelled after it is successfully resumed but
            // not dispatched yet. In case `onUndeliveredElement` is
            // specified, we need to invoke it in the latter case.
            onElementRetrieved = { element ->
                onReceiveSynchronizationCompleted()
                val onCancellation = onUndeliveredElement?.bindCancellationFun(element, cont.context)
                cont.resume(element, onCancellation)
            },
            onSuspend = { segm, i, _ -> cont.prepareReceiverForSuspension(segm, i) },
            onClosed = { onClosedReceiveOnNoWaiterSuspend(cont) },
        )
    }

    private fun CancellableContinuation<*>.prepareReceiverForSuspension(segment: ChannelSegment<E>, index: Int) {
        onReceiveEnqueued()
        onReceiveSynchronizationCompleted()
        invokeOnCancellation(SenderOrReceiverCancellationHandler(segment, index).asHandler)
    }

    private fun onClosedReceiveOnNoWaiterSuspend(cont : CancellableContinuation<E>) {
        onReceiveSynchronizationCompleted()
        cont.resumeWithException(receiveException)
    }

    /*
    The implementation is exactly the same as of `receive()`,
    with the only difference that this function returns a `ChannelResult`
    instance and does not throw exception explicitly in case the channel
    is already closed for receiving. Please refer the plain `receive()`
    implementation for the comments.
    */
    override suspend fun receiveCatching(): ChannelResult<E> =
        receiveImpl( // <-- this is an inline function
            waiter = null,
            onElementRetrieved = { element ->
                onReceiveSynchronizationCompleted()
                success(element)
            },
            onSuspend = { _, _, _ -> error("unexpected") },
            onClosed = { onClosedReceiveCatching() },
            onNoWaiterSuspend = { segm, i, r -> receiveCatchingOnNoWaiterSuspend(segm, i, r) }
        )

    private fun onClosedReceiveCatching(): ChannelResult<E> =
        closed<E>(getCloseCause()).also { onReceiveSynchronizationCompleted() }

    private suspend fun receiveCatchingOnNoWaiterSuspend(
        segment: ChannelSegment<E>,
        index: Int,
        r: Long
    ) = suspendCancellableCoroutineReusable<ChannelResult<E>> { cont ->
        val waiter = ReceiveCatching(cont)
        receiveImplOnNoWaiter(
            segment, index, r,
            waiter = waiter,
            onElementRetrieved = { element ->
                onReceiveSynchronizationCompleted()
                cont.resume(success(element), onUndeliveredElement?.bindCancellationFun(element, cont.context))
            },
            onSuspend = { segm, i, _ -> cont.prepareReceiverForSuspension(segm, i) },
            onClosed = { onClosedReceiveCatchingOnNoWaiterSuspend(cont) }
        )
    }

    private fun onClosedReceiveCatchingOnNoWaiterSuspend(cont: CancellableContinuation<ChannelResult<E>>) {
        onReceiveSynchronizationCompleted()
        cont.resume(closed(getCloseCause()))
    }

    override fun tryReceive(): ChannelResult<E> =
        tryReceiveInternal().also { onReceiveSynchronizationCompleted() }

    /**
     * This internal implementation does not invoke [onReceiveSynchronizationCompleted].
     */
    protected fun tryReceiveInternal(): ChannelResult<E> {
        // Read the `receivers` counter first.
        val r = receivers.value
        val sendersAndCloseStatusCur = sendersAndCloseStatus.value
        // Is this channel closed for receive?
        if (sendersAndCloseStatusCur.isClosedForReceive0) {
            return closed(getCloseCause())
        }
        // Do not try to receive an element if the plain `receive()` operation would suspend.
        val s = sendersAndCloseStatusCur.counter
        if (r >= s) return failure()
        // Let's try to retrieve an element!
        // The logic is similar to the plain `receive()` operation, with
        // the only difference that we store `INTERRUPTED_RCV` in case
        // the operation decides to suspend. This way, we can leverage
        // the unconditional `Fetch-and-Add` instruction.
        return receiveImpl( // <-- this is an inline function
            // Store an already interrupted receiver in case of suspension.
            waiter = INTERRUPTED_RCV,
            // Finish when an element is successfully retrieved.
            onElementRetrieved = { element -> success(element) },
            // On suspension, the `INTERRUPTED_RCV` token has been
            // installed, and this `tryReceive()` fails.
            onSuspend = { segm, _, globalIndex ->
                waitExpandBufferCompletion(globalIndex)
                segm.onSlotCleaned()
                failure()
            },
            // If the channel is closed, return the corresponding result.
            onClosed = { closed(getCloseCause()) }
        )
    }


    /**
     * Abstract receive implementation.
     */
    private inline fun <R> receiveImpl(
        /* The waiter to be stored in case of suspension,
        or `null` if the waiter is not created yet.
        In the latter case, if the algorithm decides
        to suspend, [onNoWaiterSuspend] is called. */
        waiter: Any?,
        /* This lambda is invoked when an element has been
        successfully retrieved, either from the buffer or
        by making a rendezvous with a suspended sender. */
        onElementRetrieved: (element: E) -> R,
        /* This lambda is called when the operation suspends in the cell
        specified by the segment and its global and in-segment indices. */
        onSuspend: (segm: ChannelSegment<E>, i: Int, r: Long) -> R,
        /* This lambda is called when the channel is observed
        in the closed state and no waiting sender is found,
        which means that it is closed for receiving. */
        onClosed: () -> R,
        /* This lambda is called when the operation decides
        to suspend, but the waiter is not provided (equals `null`).
        It should create a waiter and delegate to `sendImplOnNoWaiter`. */
        onNoWaiterSuspend: (
            segm: ChannelSegment<E>,
            i: Int,
            r: Long
        ) -> R = { _, _, _ -> error("unexpected") }
    ): R {
        // Read the segment reference before the counter increment;
        // it is crucial to be able to find the required segment later.
        var segment = receiveSegment.value
        while (true) {
            // Similar to the `send(e)` operation, `receive()` first checks
            // whether the channel is already closed for receiving.
            if (sendersAndCloseStatus.value.isClosedForReceive0) {
                return onClosed()
            }
            // Atomically increments the `receivers` counter
            // and obtain the value right before the increment.
            val r = this.receivers.getAndIncrement()
            // Count the required segment id and the cell index in it.
            val id = r / SEGMENT_SIZE
            val i = (r % SEGMENT_SIZE).toInt()
            // Try to find the required segment if the initially obtained
            // segment (in the beginning of this function) has lower id.
            if (segment.id != id) {
                // Find the required segment, restarting the operation if it
                // has not been found. If the channel is already cancelled or
                // closed without stored element, the operation finishes immediately.
                segment = findSegmentReceive(id, segment) ?:
                    // The required segment is not found. It is possible that the channel is already
                    // closed for receiving, so the linked list of segments is closed as well.
                    // In the latter case, the operation fails with the corresponding check at the beginning.
                    continue
            }
            // Update the cell according to the cell life-cycle.
            val updCellResult = updateCellReceive(segment, i, r, waiter)
            return when {
                updCellResult === SUSPEND -> {
                    // The operation has decided to suspend and
                    // stored the specified waiter in the cell.
                    onSuspend(segment, i, r)
                }
                updCellResult === FAILED -> {
                    // The operation has tried to make a rendezvous
                    // but failed: either the opposite request has
                    // already been cancelled or the cell is poisoned.
                    // Restart from the beginning in this case.
                    // To avoid memory leaks, we also need to reset
                    // the `prev` pointer of the working segment.
                    if (r < sendersCounter) segment.cleanPrev()
                    continue
                }
                updCellResult === SUSPEND_NO_WAITER -> {
                    // The operation has decided to suspend,
                    // but no waiter has been provided.
                    onNoWaiterSuspend(segment, i, r)
                }
                else -> { // element
                    // Either a buffered element was retrieved from the cell
                    // or a rendezvous with a waiting sender has happened.
                    // Clean the reference to the previous segment before finishing.
                    segment.cleanPrev()
                    @Suppress("UNCHECKED_CAST")
                    onElementRetrieved(updCellResult as E)
                }
            }
        }
    }

    private inline fun <W, R> receiveImplOnNoWaiter(
        /* The working cell is specified by
        the segment and the index in it. */
        segment: ChannelSegment<E>,
        index: Int,
        /* The global index of the cell. */
        r: Long,
        /* The waiter to be stored in case of suspension. */
        waiter: W,
        /* This lambda is invoked when an element has been
        successfully retrieved, either from the buffer or
        by making a rendezvous with a suspended sender. */
        onElementRetrieved: (element: E) -> R,
        /* This lambda is called when the operation suspends in the cell
        specified by the segment and its global and in-segment indices. */
        onSuspend: (segm: ChannelSegment<E>, i: Int, r: Long) -> R,
        /* This lambda is called when the channel is observed
        in the closed state and no waiting senders is found,
        which means that it is closed for receiving. */
        onClosed: () -> R
    ): R {
        // Update the cell with the non-null waiter,
        // restarting from the beginning on failure.
        // Check the `receiveImpl(..)` function for the comments.
        val updCellResult = updateCellReceive(segment, index, r, waiter)
        when {
            updCellResult === SUSPEND -> {
                return onSuspend(segment, index, r)
            }
            updCellResult === FAILED -> {
                if (r < sendersCounter) segment.cleanPrev()
                return receiveImpl(
                    waiter = waiter,
                    onElementRetrieved = onElementRetrieved,
                    onSuspend = onSuspend,
                    onClosed = onClosed
                )
            }
            else -> {
                segment.cleanPrev()
                @Suppress("UNCHECKED_CAST")
                return onElementRetrieved(updCellResult as E)
            }
        }
    }

    private fun updateCellReceive(
        /* The working cell is specified by
        the segment and the index in it. */
        segment: ChannelSegment<E>,
        index: Int,
        /* The global index of the cell. */
        r: Long,
        /* The waiter to be stored in case of suspension. */
        waiter: Any?,
    ): Any? {
        // The cell state should be updated according to  its state machine;
        // see the paper mentioned in the very beginning for the algorithm details.
        while (true) {
            // Read the current cell state.
            val state = segment.getState(index)
            when {
                // The cell is empty.
                state === null || state === IN_BUFFER -> {
                    // If a rendezvous must happen, the operation does not wait
                    // until the cell stores a buffered element or a suspended
                    // sender, poisoning the cell and restarting instead.
                    // Otherwise, try to store the specified waiter in the cell.
                    val senders = sendersAndCloseStatus.value.counter
                    if (r < senders) {
                        // The cell is already covered by sender,
                        // so a rendezvous must happen. Unfortunately,
                        // the cell is empty, so the operation poisons it.
                        if (segment.casState(index, state, POISONED)) {
                            // When the cell becomes poisoned, it is essentially
                            // the same as storing an already cancelled receiver.
                            // Thus, the `expandBuffer()` procedure should be invoked.
                            expandBuffer()
                            return FAILED
                        }
                    } else {
                        // This `receive()` operation should suspend.
                        if (waiter === null) {
                            // The waiter is not specified;
                            // return the corresponding result.
                            return SUSPEND_NO_WAITER
                        }
                        // Try to install the waiter.
                        if (segment.casState(index, state, waiter)) {
                            // The waiter has been successfully installed.
                            // Invoke the `expandBuffer()` procedure and finish.
                            expandBuffer()
                            return SUSPEND
                        }
                    }
                }
                // The cell stores a buffered element.
                state === BUFFERED -> if (segment.casState(index, state, DONE_RCV)) {
                    // Retrieve the element and expand the buffer.
                    expandBuffer()
                    return segment.retrieveElement(index)
                }
                // The cell stores an interrupted sender.
                state === INTERRUPTED_SEND -> return FAILED
                // The cell is already poisoned by a concurrent
                // `hasElements` call. Restart in this case.
                state === POISONED -> return FAILED
                // This channel is already closed.
                state === CHANNEL_CLOSED -> {
                    // Although the channel is closed, it is still required
                    // to call the `expandBuffer()` procedure to keep
                    // `waitForExpandBufferCompletion()` correct.
                    expandBuffer()
                    return FAILED
                }
                // A concurrent `expandBuffer()` is resuming a
                // suspended sender. Wait in a spin-loop until
                // the resumption attempt completes: the cell
                // state must change to either `BUFFERED` or
                // `INTERRUPTED_SEND`.
                state === S_RESUMING_EB -> continue
                // The cell stores a suspended sender; try to resume it.
                else -> {
                    // To synchronize with expandBuffer(), the algorithm
                    // first moves the cell to an intermediate `S_RESUMING_RCV`
                    // state, updating it to either `BUFFERED` (on success) or
                    // `INTERRUPTED_SEND` (on failure).
                    if (segment.casState(index, state, S_RESUMING_RCV)) {
                        // Has a concurrent `expandBuffer()` delegated its completion?
                        val helpExpandBuffer = state is WaiterEB
                        // Extract the sender if needed and try to resume it.
                        val sender = if (state is WaiterEB) state.waiter else state
                        return if (sender.tryResumeSender(segment, index)) {
                            // The sender has been resumed successfully!
                            // Update the cell state correspondingly,
                            // expand the buffer, and return the element
                            // stored in the cell.
                            // In case a concurrent `expandBuffer()` has delegated
                            // its completion, the procedure should finish, as the
                            // sender is resumed. Thus, no further action is required.
                            segment.setState(index, DONE_RCV)
                            expandBuffer()
                            segment.retrieveElement(index)
                        } else {
                            // The resumption has failed. Update the cell correspondingly.
                            // In case a concurrent `expandBuffer()` has delegated
                            // its completion, the procedure should skip this cell, so
                            // `expandBuffer()` should be called once again.
                            segment.setState(index, INTERRUPTED_SEND)
                            segment.onCancelledRequest(index, false)
                            if (helpExpandBuffer) expandBuffer()
                            FAILED
                        }
                    }
                }
            }
        }
    }

    private fun Any.tryResumeSender(segment: ChannelSegment<E>, index: Int): Boolean = when (this) {
        is CancellableContinuation<*> -> { // suspended `send(e)` operation
            @Suppress("UNCHECKED_CAST")
            this as CancellableContinuation<Unit>
            tryResume0(Unit)
        }
        is SelectInstance<*> -> {
            this as SelectImplementation<*>
            val trySelectResult = trySelectDetailed(clauseObject = this@BufferedChannel, result = Unit)
            if (trySelectResult === REREGISTER) segment.cleanElement(index)
            trySelectResult === SUCCESSFUL
        }
        is SendBroadcast -> cont.tryResume0(true) // // suspended `sendBroadcast(e)` operation
        else -> error("Unexpected waiter: $this")
    }

    // ################################
    // # The expandBuffer() procedure #
    // ################################

    private fun expandBuffer() {
        // Do not need to take any action if
        // this channel is rendezvous or unlimited.
        if (isRendezvousOrUnlimited) return
        // Read the current segment of
        // the `expandBuffer()` procedure.
        var segment = bufferEndSegment.value
        // Try to expand the buffer until succeed.
        try_again@ while (true) {
            // Increment the logical end of the buffer.
            // The `b`-th cell is going to be added to the buffer.
            val b = bufferEnd.getAndIncrement()
            val id = b / SEGMENT_SIZE
            // After that, read the current `senders` counter.
            // In case its value is lower than `b`, the `send(e)`
            // invocation that will work with this `b`-th cell
            // will detect that the cell is already a part of the
            // buffer when comparing with the `bufferEnd` counter.
            // However, `bufferEndSegment` may reference an outdated
            // segment, which should be updated to avoid memory leaks.
            val s = sendersCounter
            if (s <= b) {
                // Should `bufferEndSegment` be updated?
                if (segment.id < id && segment.next != null) {
                    bufferEndSegment.moveForward(findSegmentBufferOrLast(id, segment))
                    incCompletedExpandBuffers()
                    return
                } // to avoid memory leaks
                incCompletedExpandBuffers()
                return
            }
            val i = (b % SEGMENT_SIZE).toInt()
            if (segment.id != id) {
                segment = findSegmentBuffer(id, segment).let {
                    if (it.isClosed) {
                        val ss = findSegmentBufferOrLast(id, segment)
                        bufferEndSegment.value = ss
                        incCompletedExpandBuffers()
                        return
                    } else {
                        it.segment
                    }
                }
            }
            if (segment.id != id) {
                if (bufferEnd.compareAndSet(b + 1, segment.id * SEGMENT_SIZE)) {
                    incCompletedExpandBuffers(segment.id * SEGMENT_SIZE - b)
                } else {
                    incCompletedExpandBuffers()
                }
                continue@try_again
            }
            if (updateCellExpandBuffer(segment, i, b)) {
                incCompletedExpandBuffers()
                return
            } else {
                incCompletedExpandBuffers()
            }
        }
    }

    private fun updateCellExpandBuffer(
        /* The working cell is specified by
        the segment and the index in it. */
        segment: ChannelSegment<E>,
        index: Int,
        /* The global index of the cell. */
        b: Long
    ): Boolean {
        // Update the cell state according to its state machine.
        // See the paper mentioned in the very beginning for
        // the cell life-cycle and the algorithm details.
        while (true) {
            // Read the current cell state.
            val state = segment.getState(index)
            when {
                // A suspended waiter, sender or receiver.
                state is Waiter -> {
                    // Usually, a sender is stored in the cell.
                    // However, it is possible for a concurrent
                    // receiver to be already suspended there.
                    // Try to distinguish whether the waiter is a
                    // sender by comparing the global cell index with
                    // the `receivers` counter. In case the cell is not
                    // covered by a receiver, a sender is stored in the cell.
                    if (b < receivers.value) {
                        // The algorithm cannot distinguish whether the
                        // suspended in the cell operation is sender or receiver.
                        // To make progress, `expandBuffer()` delegates its completion
                        // to an upcoming pairwise request, atomically wrapping
                        // the waiter in `WaiterEB`. In case a sender is stored
                        // in the cell, the upcoming receiver will call `expandBuffer()`
                        // if the sender resumption fails; thus, effectively, skipping
                        // this cell. Otherwise, if a receiver is stored in the cell,
                        // this `expandBuffer()` procedure must finish; therefore,
                        // sender ignore the `WaiterEB` wrapper.
                        if (segment.casState(index, state, WaiterEB(waiter = state)))
                            return true
                    } else {
                        // The cell stores a suspended sender. Try to resume it.
                        // To synchronize with a concurrent `receive()`, the algorithm
                        // first moves the cell state to an intermediate `S_RESUMING_EB`
                        // state, updating it to either `BUFFERED` (on successful resumption)
                        // or `INTERRUPTED_SEND` (on failure).
                        if (segment.casState(index, state, S_RESUMING_EB)) {
                            return if (state.tryResumeSender(segment, index)) {
                                // The sender has been resumed successfully!
                                // Move the cell to the logical buffer and finish.
                                segment.setState(index, BUFFERED)
                                true
                            } else {
                                // The resumption has failed.
                                segment.setState(index, INTERRUPTED_SEND)
                                segment.onCancelledRequest(index, false)
                                false
                            }
                        }
                    }
                }
                // The cell stores an interrupted sender, skip it.
                state === INTERRUPTED_SEND -> return false
                // The cell is empty, a concurrent sender is coming.
                state === null -> {
                    // To inform a concurrent sender that this cell is
                    // already a part of the buffer, the algorithm moves
                    // it to a special `IN_BUFFER` state.
                    if (segment.casState(index, state, IN_BUFFER)) return true
                }
                // The cell is already a part of the buffer, finish.
                state === BUFFERED -> return true
                // The cell is already processed by a receiver, no further action is required.
                state === POISONED || state === DONE_RCV || state === INTERRUPTED_RCV -> return true
                // The channel is closed, all the following
                // cells are already in the same state, finish.
                state === CHANNEL_CLOSED -> return true
                // A concurrent receiver is resuming the suspended sender.
                // Wait in a spin-loop until it changes the cell state
                // to either `DONE_RCV` or `INTERRUPTED_SEND`.
                state === S_RESUMING_RCV -> continue // spin wait
                else -> error("Unexpected cell state: $state")
            }
        }
    }

    private fun incCompletedExpandBuffers(nAttempts: Long = 1) {
        completedExpandBuffers.addAndGet(nAttempts).also {
            if (it.closeStatus != 0) {
                while (completedExpandBuffers.value.closeStatus != 0) {}
            }
        }
    }

    internal fun waitExpandBufferCompletion(globalIndex: Long) {
        if (isRendezvousOrUnlimited) return
        repeat(EXPAND_BUFFER_COMPLETION_WAIT_ITERATIONS) {
            var b = bufferEnd.value
            while (b <= globalIndex) { b = bufferEnd.value }
            val ebCompleted = completedExpandBuffers.value.counter
            if (b == ebCompleted && b == bufferEnd.value) return
        }
        completedExpandBuffers.update { constructSendersAndCloseStatus(it.counter, 1) }
        while (true) {
            val b = bufferEnd.value
            val ebCompletedAndBit = completedExpandBuffers.value
            val ebCompleted = ebCompletedAndBit.counter
            if (b == ebCompleted && b == bufferEnd.value) {
                completedExpandBuffers.update { constructSendersAndCloseStatus(it.counter, 0) }
                return
            }
            if (ebCompletedAndBit.closeStatus == 0) {
                completedExpandBuffers.compareAndSet(ebCompletedAndBit, constructSendersAndCloseStatus(ebCompleted, 1))
            }
        }
    }

    private fun findSegmentBufferOrLast(id: Long, startFrom: ChannelSegment<E>): ChannelSegment<E> {
        var cur: ChannelSegment<E> = startFrom
        while (cur.id < id) {
            cur = cur.next ?: break
        }
        while (cur.isRemoved) {
            cur = cur.next ?: break
        }
        return cur
    }


    // #######################
    // ## Select Expression ##
    // #######################

    @Suppress("UNCHECKED_CAST")
    override val onSend: SelectClause2<E, BufferedChannel<E>>
        get() = SelectClause2Impl(
            clauseObject = this@BufferedChannel,
            regFunc = BufferedChannel<*>::registerSelectForSend as RegistrationFunction,
            processResFunc = BufferedChannel<*>::processResultSelectSend as ProcessResultFunction
        )

    @Suppress("UNCHECKED_CAST")
    protected open fun registerSelectForSend(select: SelectInstance<*>, element: Any?) =
        sendImpl( // <-- this is an inline function
            element = element as E,
            waiter = select,
            onRendezvousOrBuffered = { select.selectInRegistrationPhase(Unit) },
            onSuspend = { segm, i -> select.prepareSenderForSuspension(segm, i) },
            onClosed = { onClosedSelectOnSend(element, select) }
        )

    private fun SelectInstance<*>.prepareSenderForSuspension(
        // The working cell is specified by
        // the segment and the index in it.
        segment: ChannelSegment<E>,
        index: Int
    ) {
        if (onUndeliveredElement == null) {
            // TODO: replace with a more efficient cancellation
            // TODO: mechanism  for segments when #3084 is finished.
            disposeOnCompletion(SenderOrReceiverCancellationHandler(segment, index))
        } else {
            disposeOnCompletion(SenderWithOnUndeliveredElementCancellationHandler(segment, index, context))
        }
    }

    private fun onClosedSelectOnSend(element: E, select: SelectInstance<*>) {
        onUndeliveredElement?.callUndeliveredElement(element, select.context)
        select.selectInRegistrationPhase(CHANNEL_CLOSED)
    }

    @Suppress("UNUSED_PARAMETER", "RedundantNullableReturnType")
    private fun processResultSelectSend(ignoredParam: Any?, selectResult: Any?): Any? =
        if (selectResult === CHANNEL_CLOSED) throw sendException
        else this

    @Suppress("UNCHECKED_CAST")
    override val onReceive: SelectClause1<E>
        get() = SelectClause1Impl(
            clauseObject = this@BufferedChannel,
            regFunc = BufferedChannel<*>::registerSelectForReceive as RegistrationFunction,
            processResFunc = BufferedChannel<*>::processResultSelectReceive as ProcessResultFunction,
            onCancellationConstructor = onUndeliveredElementReceiveCancellationConstructor
        )

    @Suppress("UNCHECKED_CAST")
    override val onReceiveCatching: SelectClause1<ChannelResult<E>>
        get() = SelectClause1Impl(
            clauseObject = this@BufferedChannel,
            regFunc = BufferedChannel<*>::registerSelectForReceive as RegistrationFunction,
            processResFunc = BufferedChannel<*>::processResultSelectReceiveCatching as ProcessResultFunction,
            onCancellationConstructor = onUndeliveredElementReceiveCancellationConstructor
        )

    @Suppress("OVERRIDE_DEPRECATION", "UNCHECKED_CAST")
    override val onReceiveOrNull: SelectClause1<E?>
        get() = SelectClause1Impl(
            clauseObject = this@BufferedChannel,
            regFunc = BufferedChannel<*>::registerSelectForReceive as RegistrationFunction,
            processResFunc = BufferedChannel<*>::processResultSelectReceiveOrNull as ProcessResultFunction,
            onCancellationConstructor = onUndeliveredElementReceiveCancellationConstructor
        )

    protected open fun registerSelectForReceive(select: SelectInstance<*>, ignoredParam: Any?) =
        receiveImpl( // <-- this is an inline function
            waiter = select,
            onElementRetrieved = { elem ->
                onReceiveSynchronizationCompleted()
                select.selectInRegistrationPhase(elem)
            },
            onSuspend = { segm, i, _ -> select.prepareReceiverForSuspension(segm, i) },
            onClosed = { onClosedSelectOnReceive(select) }
        )

    private fun SelectInstance<*>.prepareReceiverForSuspension(
        /* The working cell is specified by
        the segment and the index in it. */
        segment: ChannelSegment<E>,
        index: Int
    ) {
        onReceiveSynchronizationCompleted()
        onReceiveEnqueued()
        disposeOnCompletion(SenderOrReceiverCancellationHandler(segment, index))
    }

    private fun onClosedSelectOnReceive(select: SelectInstance<*>) {
        onReceiveSynchronizationCompleted()
        select.selectInRegistrationPhase(CHANNEL_CLOSED)
    }

    @Suppress("UNUSED_PARAMETER")
    private fun processResultSelectReceive(ignoredParam: Any?, selectResult: Any?): Any? =
        if (selectResult === CHANNEL_CLOSED) throw receiveException
        else selectResult

    @Suppress("UNUSED_PARAMETER")
    private fun processResultSelectReceiveOrNull(ignoredParam: Any?, selectResult: Any?): Any? =
        if (selectResult === CHANNEL_CLOSED) {
            if (getCloseCause() == null) null
            else throw receiveException
        } else selectResult

    @Suppress("UNCHECKED_CAST", "UNUSED_PARAMETER", "RedundantNullableReturnType")
    private fun processResultSelectReceiveCatching(ignoredParam: Any?, selectResult: Any?): Any? =
        if (selectResult === CHANNEL_CLOSED) closed(getCloseCause())
        else success(selectResult as E)

    @Suppress("UNCHECKED_CAST")
    private val onUndeliveredElementReceiveCancellationConstructor: OnCancellationConstructor? = onUndeliveredElement?.let {
        { select: SelectInstance<*>, _: Any?, element: Any? ->
            { if (element !== CHANNEL_CLOSED) onUndeliveredElement.callUndeliveredElement(element as E, select.context) }
        }
    }

    // ######################
    // ## Iterator Support ##
    // ######################

    override fun iterator(): ChannelIterator<E> = BufferedChannelIterator()

    /**
     * The key idea is that an iterator is a special receiver type,
     * which should be resumed differently to [receive] and [onReceive]
     * operations, but can be served as a waiter in a way similar to
     * [CancellableContinuation] and [SelectInstance].
     *
     * Roughly, [hasNext] is a [receive] sibling, while [next] simply
     * returns the already retrieved element. From the implementation
     * side, [receiveResult] stores the element retrieved by [hasNext]
     * (or a special [ClosedChannel] token if the channel is closed).
     *
     * The [invoke] function is a [CancelHandler] implementation, the
     * implementation of which requires storing the [segment] and the
     * [index] in it that specify the location of the stored iterator.
     *
     * To resume the suspended [hasNext] call, a special [tryResumeHasNext]
     * function should be used in a way similar to [CancellableContinuation.tryResume]
     * and [SelectInstance.trySelect]. When the channel becomes closed,
     * [tryResumeHasNextWithCloseException] should be used instead.
     */
    protected open inner class BufferedChannelIterator : ChannelIterator<E>, BeforeResumeCancelHandler(), Waiter {
        private var receiveResult: Any? = NO_RECEIVE_RESULT

        private var cont: CancellableContinuation<Boolean>? = null

        private var segment: ChannelSegment<E>? = null
        private var index = -1

        // on cancellation
        override fun invoke(cause: Throwable?) {
            segment?.onCancellation(index)
            onReceiveDequeued()
        }

        override suspend fun hasNext(): Boolean = receiveImpl(
            waiter = null,
            onElementRetrieved = { element ->
                this.receiveResult = element
                onReceiveSynchronizationCompleted()
                true
            },
            onSuspend = { _, _, _ -> error("unreachable") },
            onClosed = { onCloseHasNext() },
            onNoWaiterSuspend = { segm, i, r -> return hasNextOnNoWaiterSuspend(segm, i, r) }
        )

        private fun onCloseHasNext(): Boolean {
            val cause = getCloseCause()
            onReceiveSynchronizationCompleted()
            this.receiveResult = CHANNEL_CLOSED
            if (cause == null) return false
            else throw recoverStackTrace(cause)
        }

        private suspend fun hasNextOnNoWaiterSuspend(
            segm: ChannelSegment<E>,
            i: Int,
            r: Long
        ): Boolean = suspendCancellableCoroutineReusable { cont ->
            this.cont = cont
            receiveImplOnNoWaiter(
                segm, i, r,
                waiter = this,
                onElementRetrieved = { element ->
                    this.receiveResult = element
                    this.cont = null
                    onReceiveSynchronizationCompleted()
                    cont.resume(true) {
                        onUndeliveredElement?.callUndeliveredElement(element, cont.context)
                    }
                },
                onSuspend = { segment, i, _ ->
                    this.segment = segment
                    this.index = i
                    cont.invokeOnCancellation(this.asHandler)
                    onReceiveEnqueued()
                    onReceiveSynchronizationCompleted()
                },
                onClosed = {
                    this.cont = null
                    val cause = getCloseCause()
                    this.receiveResult = CHANNEL_CLOSED
                    onReceiveSynchronizationCompleted()
                    if (cause == null) {
                        cont.resume(false)
                    } else {
                        cont.resumeWithException(recoverStackTrace(cause))
                    }
                }
            )
        }

        @Suppress("UNCHECKED_CAST")
        override fun next(): E {
            // Read the already received result, or [NO_RECEIVE_RESULT] if [hasNext] has not been invoked yet.
            check(receiveResult !== NO_RECEIVE_RESULT) { "`hasNext()` has not been invoked" }
            val result = receiveResult
            receiveResult = NO_RECEIVE_RESULT
            // Is this channel closed?
            if (result === CHANNEL_CLOSED) throw recoverStackTrace(receiveException)
            // Return the element.
            return result as E
        }

        fun tryResumeHasNext(element: E): Boolean {
            this.receiveResult = element
            val cont = this.cont!!
            this.cont = null
            return cont.tryResume(true, null, onUndeliveredElement?.bindCancellationFun(element, cont.context)).let {
                if (it !== null) {
                    cont.completeResume(it)
                    true
                } else false
            }
        }

        fun tryResumeHasNextWithCloseException() {
            this.receiveResult = CHANNEL_CLOSED
            val cont = cont!!
            if (getCloseCause() == null) {
                cont.resume(false)
            } else {
                cont.resumeWithException(receiveException)
            }
        }
    }

    // ##############################
    // ## Closing and Cancellation ##
    // ##############################

    /**
     * Indicates whether this channel is cancelled. In case it is cancelled,
     * it stores either an exception if it was cancelled with or `null` if
     * this channel was cancelled without error. Stores [NO_CLOSE_CAUSE] if this
     * channel is not cancelled.
     */
    private val closeCause = atomic<Any?>(NO_CLOSE_CAUSE)

    protected fun getCloseCause() = closeCause.value as Throwable?

    protected val sendException get() =
        getCloseCause() ?: ClosedSendChannelException(DEFAULT_CLOSE_MESSAGE)

    private val receiveException get() =
        getCloseCause() ?: ClosedReceiveChannelException(DEFAULT_CLOSE_MESSAGE)

    // Stores the close handler.
    private val closeHandler = atomic<Any?>(null)

    private fun markClosed(): Unit =
        sendersAndCloseStatus.update { cur ->
            when (cur.closeStatus) {
                CLOSE_STATUS_ACTIVE ->
                    constructSendersAndCloseStatus(cur.counter, CLOSE_STATUS_CLOSED)
                CLOSE_STATUS_CANCELLATION_STARTED ->
                    constructSendersAndCloseStatus(cur.counter, CLOSE_STATUS_CANCELLED)
                else -> return
            }
        }.also { assert { closeCause.value is Throwable? } }

    private fun markCancelled(): Unit =
        sendersAndCloseStatus.update { cur ->
            constructSendersAndCloseStatus(cur.counter, CLOSE_STATUS_CANCELLED)
        }

    private fun markCancellationStarted(): Unit =
        sendersAndCloseStatus.update { cur ->
            if (cur.closeStatus == CLOSE_STATUS_ACTIVE)
                constructSendersAndCloseStatus(cur.counter, CLOSE_STATUS_CANCELLATION_STARTED)
            else return
        }

    private fun completeCloseOrCancel() {
        sendersAndCloseStatus.value.isClosedForSend0
    }

    override fun close(cause: Throwable?): Boolean = closeImpl(cause, false)

    protected open fun closeImpl(cause: Throwable?, cancel: Boolean): Boolean {
        if (cancel) markCancellationStarted()
        val closedByThisOperation = closeCause.compareAndSet(NO_CLOSE_CAUSE, cause)
        if (cancel) markCancelled() else markClosed()
        completeCloseOrCancel()
        return if (closedByThisOperation) {
            invokeCloseHandler()
            true
        } else false
    }

    private fun completeClose(sendersCur: Long) {
        val lastSegment = closeQueue()
        cancelWaitingReceiveRequests(lastSegment, sendersCur)
        onClosedIdempotent()
    }

    private fun completeCancel(sendersCur: Long) {
        completeClose(sendersCur)
        removeRemainingBufferedElements()
    }

    private fun closeQueue(): ChannelSegment<E> {
        // Choose the last segment.
        val lastSegment = listOf(bufferEndSegment.value, sendSegment.value, receiveSegment.value).maxBy { it.id }
        // Close the linked list of segment for new segment addition
        // and return the last segment at the point of closing.
        return lastSegment.close()
    }

    @Suppress("UNCHECKED_CAST")
    private fun invokeCloseHandler() {
        val closeHandler = closeHandler.getAndUpdate {
            if (it === null) CLOSE_HANDLER_CLOSED
            else CLOSE_HANDLER_INVOKED
        } ?: return
        closeHandler as (cause: Throwable?) -> Unit
        closeHandler(getCloseCause())
    }

    override fun invokeOnClose(handler: (cause: Throwable?) -> Unit) {
        if (closeHandler.compareAndSet(null, handler)) {
            // Handler has been successfully set, finish the operation.
            return
        }
        // Either handler was set already or this channel is cancelled.
        // Read the value of [closeHandler] and either throw [IllegalStateException]
        // or invoke the handler respectively.
        when (val curHandler = closeHandler.value) {
            CLOSE_HANDLER_CLOSED -> {
                // In order to be sure that our handler is the only one, we have to change the
                // [closeHandler] value to `INVOKED`. If this CAS fails, another handler has been
                // executed and an [IllegalStateException] should be thrown.
                if (closeHandler.compareAndSet(CLOSE_HANDLER_CLOSED, CLOSE_HANDLER_INVOKED)) {
                    handler(getCloseCause())
                } else {
                    throw IllegalStateException("Another handler was already registered and successfully invoked")
                }
            }
            CLOSE_HANDLER_INVOKED -> {
                throw IllegalStateException("Another handler was already registered and successfully invoked")
            }
            else -> {
                throw IllegalStateException("Another handler was already registered: $curHandler")
            }
        }
    }

    /**
     * Invoked when channel is closed as the last action of [close] invocation.
     * This method should be idempotent and can be called multiple times.
     */
    protected open fun onClosedIdempotent() {}

    protected open fun onCancel(wasClosed: Boolean) {}

    @Suppress("OVERRIDE_DEPRECATION")
    final override fun cancel(cause: Throwable?): Boolean = cancelImpl(cause)

    @Suppress("OVERRIDE_DEPRECATION")
    final override fun cancel() { cancelImpl(null) }

    final override fun cancel(cause: CancellationException?) { cancelImpl(cause) }

    internal open fun cancelImpl(cause: Throwable?): Boolean {
        val materializedCause = cause ?: CancellationException("Channel was cancelled")
        val wasClosed = closeImpl(materializedCause, true)
        removeRemainingBufferedElements()
        onCancel(wasClosed)
        return wasClosed
    }

    private fun removeRemainingBufferedElements() {
        // clear buffer first, but do not wait for it in helpers
        val onUndeliveredElement = onUndeliveredElement
        var undeliveredElementException: UndeliveredElementException? = null // first cancel exception, others suppressed

        var segm: ChannelSegment<E> = sendSegment.value
        while (true) {
            segm = segm.next ?: break
        }
        val limit = receivers.value
        while (true) {
            for (index in SEGMENT_SIZE - 1 downTo 0) {
                if (segm.id * SEGMENT_SIZE + index < limit) return
                update_cell@while (true) {
                    val state = segm.getState(index)
                    when {
                        state === DONE_RCV -> break@update_cell
                        state === BUFFERED -> if (segm.casState(index, state, CHANNEL_CLOSED)) {
                            if (onUndeliveredElement != null) {
                                undeliveredElementException = onUndeliveredElement.callUndeliveredElementCatchingException(segm.retrieveElement(index), undeliveredElementException)
                            }
                            segm.cleanElement(index)
                            segm.onSlotCleaned()
                            break@update_cell
                        }
                        state === IN_BUFFER || state === null -> if (segm.casState(index, state, CHANNEL_CLOSED)) {
                            segm.onSlotCleaned()
                            break@update_cell
                        }
                        state is Waiter -> {
                            if (segm.casState(index, state, CHANNEL_CLOSED)) {
                                if (onUndeliveredElement != null) {
                                    undeliveredElementException = onUndeliveredElement.callUndeliveredElementCatchingException(segm.retrieveElement(index), undeliveredElementException)
                                }
                                segm.cleanElement(index)
                                state.closeSender()
                                segm.onSlotCleaned()
                                break@update_cell
                            }
                        }
                        state is WaiterEB -> {
                            if (segm.casState(index, state, CHANNEL_CLOSED)) {
                                if (onUndeliveredElement != null) {
                                    undeliveredElementException = onUndeliveredElement.callUndeliveredElementCatchingException(segm.retrieveElement(index), undeliveredElementException)
                                }
                                state.waiter.closeSender()
                                segm.cleanElement(index)
                                segm.onSlotCleaned()
                                break@update_cell
                            }
                        }
                        state === S_RESUMING_EB || state === S_RESUMING_RCV -> continue@update_cell
                        else -> break@update_cell
                    }
                }
            }
            segm = segm.prev ?: break
        }
        undeliveredElementException?.let { throw it } // throw UndeliveredElementException at the end if there was one
    }

    private fun cancelWaitingReceiveRequests(lastSegment: ChannelSegment<E>, sendersCur: Long) {
        var segm: ChannelSegment<E>? = lastSegment
        while (segm != null) {
            for (i in SEGMENT_SIZE - 1 downTo 0) {
                if (segm.id * SEGMENT_SIZE + i < sendersCur) return
                cell@while (true) {
                    val state = segm.getState(i)
                    when {
                        state === null || state === IN_BUFFER -> {
                            if (segm.casState(i, state, CHANNEL_CLOSED)) {
                                segm.onSlotCleaned()
                                break@cell
                            }
                        }
                        state is WaiterEB -> {
                            if (segm.casState(i, state, CHANNEL_CLOSED)) {
                                state.waiter.closeReceiver()
                                segm.onSlotCleaned()
                                break@cell
                            }
                        }
                        state is Waiter -> {
                            if (segm.casState(i, state, CHANNEL_CLOSED)) {
                                state.closeReceiver()
                                segm.onSlotCleaned()
                                break@cell
                            }
                        }
                        else -> break@cell
                    }
                }
            }
            segm = segm.prev
        }
    }

    private fun Waiter.closeReceiver() = closeWaiter(receiver = true)
    private fun Any.closeSender() = closeWaiter(receiver = false)

    private fun Any.closeWaiter(receiver: Boolean): Boolean {
        return when (this) {
            is SendBroadcast -> {
                this.cont.resume(false)
                true
            }
            is CancellableContinuation<*> -> {
                val exception = if (receiver) receiveException else sendException
                this.tryResumeWithException(exception)?.also { this.completeResume(it) }.let { it !== null }
            }
            is ReceiveCatching<*> -> {
                this.cont.tryResume(closed(getCloseCause()))?.also { this.cont.completeResume(it) }.let { it !== null }
            }
            is BufferedChannel<*>.BufferedChannelIterator -> {
                this.tryResumeHasNextWithCloseException()
                true
            }
            is SelectInstance<*> -> this.trySelect(this@BufferedChannel, CHANNEL_CLOSED)
            else -> error("Unexpected waiter: $this")
        }
    }

    @ExperimentalCoroutinesApi
    override val isClosedForSend: Boolean
        get() = sendersAndCloseStatus.value.isClosedForSend0

    private val Long.isClosedForSend0 get() =
        isClosed(this, sendersCur = this.counter, isClosedForReceive = false)

    @ExperimentalCoroutinesApi
    override val isClosedForReceive: Boolean
        get() = sendersAndCloseStatus.value.isClosedForReceive0

    private val Long.isClosedForReceive0 get() =
        isClosed(this, sendersCur = this.counter, isClosedForReceive = true)

    private fun isClosed(
        sendersAndCloseStatusCur: Long,
        sendersCur: Long,
        isClosedForReceive: Boolean
    ) = when (sendersAndCloseStatusCur.closeStatus) {
        // This channel is active and has not been closed.
        CLOSE_STATUS_ACTIVE -> false
        // The cancellation procedure has been started but
        // not linearized yet, so this channel should be
        // considered as active.
        CLOSE_STATUS_CANCELLATION_STARTED -> false
        // This channel has been successfully closed.
        // Help to complete the closing procedure to
        // guarantee linearizability, and return `true`
        // for senders or the flag whether there still
        // exist elements to retrieve for receivers.
        CLOSE_STATUS_CLOSED -> {
            completeClose(sendersCur)
            // When `isClosedForReceive` is `false`, always return `true`.
            // Otherwise, it is possible that the channel is closed but
            // still has elements to retrieve.
            if (isClosedForReceive) !hasElements() else true
        }
        // This channel has been successfully cancelled.
        // Help to complete the cancellation procedure to
        // guarantee linearizability and return `true`.
        CLOSE_STATUS_CANCELLED -> {
            completeCancel(sendersCur)
            true
        }
        else -> error("unexpected close status: ${sendersAndCloseStatusCur.closeStatus}")
    }

    @ExperimentalCoroutinesApi
    override val isEmpty: Boolean get() =
        if (sendersAndCloseStatus.value.isClosedForReceive0) false
        else if (hasElements()) false
        else !sendersAndCloseStatus.value.isClosedForReceive0

    /**
     * Checks whether this channel contains elements to retrieve.
     * Unfortunately, simply comparing the counters is not sufficient,
     * as there can be cells in INTERRUPTED state due to cancellation.
     * Therefore, this function tries to fairly find the first element,
     * updating the `receivers` counter correspondingly.
     */
    internal fun hasElements(): Boolean {
        // Read the segment before accessing `receivers` counter.
        var segm = receiveSegment.value
        while (true) {
            // Is there a chance that this channel has elements?
            val r = receivers.value
            val s = sendersAndCloseStatus.value.counter
            if (s <= r) return false // no elements
            // Try to access the `r`-th cell.
            // Get the corresponding segment first.
            val id = r / SEGMENT_SIZE
            if (segm.id != id) {
                // Find the required segment, and retry the operation when
                // the segment with the specified id has not been found
                // due to be full of cancelled cells. Also, when the segment
                // has not been found and the channel is already closed,
                // complete with `false`.
                segm = findSegmentHasElements(id, segm).let {
                    if (it.isClosed) return false
                    if (!isRendezvousOrUnlimited && it.segment.id <= bufferEndCounter / SEGMENT_SIZE) {
                        bufferEndSegment.moveForward(it.segment)
                    }
                    if (it.segment.id != id) {
                        updateReceiversIfLower(it.segment.id * SEGMENT_SIZE)
                        null
                    } else it.segment
                } ?: continue
            }
            segm.cleanPrev()
            // Does the `r`-th cell contain waiting sender or buffered element?
            val i = (r % SEGMENT_SIZE).toInt()
            if (!isCellEmpty(segm, i, r)) {
                return true
            }
            // The cell is empty. Update `receivers` counter and try again.
            receivers.compareAndSet(r, r + 1)
        }
    }

    /**
     * Checks whether this cell contains a buffered element
     * or a waiting sender, returning `false` in this case.
     * Otherwise, if this cell is empty (due to waiter cancellation,
     * channel closing, or marking it as `POISONED`), the operation
     * returns `true`.
     */
    private fun isCellEmpty(
        segm: ChannelSegment<E>,
        i: Int, // the cell index in `segm`
        r: Long // the global cell index
    ): Boolean {
        // The logic is similar to `updateCellReceive` with the only difference
        // that this operation does not change the state and retrieve the element.
        // TODO: simplify the conditions and document them.
        while (true) {
            val state = segm.getState(i)
            when {
                state === null || state === IN_BUFFER -> {
                    if (segm.casState(i, state, POISONED)) {
                        expandBuffer()
                        return true
                    }
                }
                state === BUFFERED -> {
                    return false
                }
                state === INTERRUPTED_SEND -> return true
                state === CHANNEL_CLOSED -> return true
                state === DONE_RCV -> return true
                state === POISONED -> return true
                state === S_RESUMING_EB || state === S_RESUMING_RCV -> continue // spin-wait
                else -> return receivers.value != r
            }
        }
    }

    // #######################
    // # Segments Management #
    // #######################

    private fun findSegmentSend(id: Long, start: ChannelSegment<E>, closed: Boolean): ChannelSegment<E>? {
        return sendSegment.findSegmentAndMoveForward(id, start, ::createSegment).let {
            if (it.isClosed) {
                if (start.id * SEGMENT_SIZE <  receiversCounter) start.cleanPrev()
                completeCloseOrCancel()
                null
            } else {
                val segm = it.segment
                if (segm.id != id) {
                    assert { segm.id > id }
                    if (segm.id * SEGMENT_SIZE <  receiversCounter) segm.cleanPrev()
                    updateSendersIfLower(segm.id * SEGMENT_SIZE)
                    if (closed && id * SEGMENT_SIZE < receiversCounter) {
                        segm.cleanPrev()
                    }
                    null
                } else segm
            }
        }
    }

    private fun findSegmentReceive(id: Long, start: ChannelSegment<E>) =
        receiveSegment.findSegmentAndMoveForward(id, start, ::createSegment).let {
            if (it.isClosed) {
                if (start.id * SEGMENT_SIZE < sendersCounter) start.cleanPrev()
                completeCloseOrCancel()
                null
            } else {
                val segm = it.segment
                if (segm.id != id) {
                    assert { segm.id > id }
                    if (segm.id * SEGMENT_SIZE <  sendersCounter) segm.cleanPrev()
                    updateReceiversIfLower(segm.id * SEGMENT_SIZE)
                    null
                } else segm
            }
        }

    private fun findSegmentHasElements(id: Long, start: ChannelSegment<E>) =
        receiveSegment.findSegmentAndMoveForward(id, start, ::createSegment)

    private fun findSegmentBuffer(id: Long, start: ChannelSegment<E>) =
        bufferEndSegment.findSegmentAndMoveForward(id, start, ::createSegment)

    private fun updateSendersIfLower(value: Long): Unit =
        sendersAndCloseStatus.loop { cur ->
            val curCounter = cur.counter
            if (curCounter >= value) return
            val update = constructSendersAndCloseStatus(curCounter, cur.closeStatus)
            if (sendersAndCloseStatus.compareAndSet(cur, update)) return
        }

    private fun updateReceiversIfLower(value: Long): Unit =
        receivers.loop { cur ->
            if (cur >= value) return
            if (receivers.compareAndSet(cur, value)) return
        }

    // ##################
    // # FOR DEBUG INFO #
    // ##################

    internal val receiversCounter: Long get() = receivers.value
    internal val bufferEndCounter: Long get() = bufferEnd.value
    internal val sendersCounter: Long get() = sendersAndCloseStatus.value.counter

    // Returns a debug representation of this channel,
    // which we actively use in Lincheck tests.
    override fun toString(): String {
        val sb = StringBuilder()
        val firstSegment = listOf(receiveSegment.value, sendSegment.value, bufferEndSegment.value)
            .filter { it !== NULL_SEGMENT }
            .minBy { it.id }
        var cur = firstSegment
        sb.append("S=${sendersAndCloseStatus.value.counter},R=${receivers.value},B=${bufferEnd.value},B'=${completedExpandBuffers.value},C=${sendersAndCloseStatus.value.closeStatus},")
        sb.append("S_SEGM=${sendSegment.value.hexAddress},R_SEGM=${receiveSegment.value.hexAddress},B_SEGM=${bufferEndSegment.value.let { if (it === NULL_SEGMENT) "NULL" else it.hexAddress }}  ")
        while (true) {
            sb.append("${cur.hexAddress}=[${if (cur.isRemoved) "*" else ""}${cur.id},prev=${cur.prev?.hexAddress},")
            repeat(SEGMENT_SIZE) { i ->
                val w = cur.getState(i)
                val e = cur.getElement(i)
                val wString = when (w) {
                    is CancellableContinuation<*> -> "cont"
                    is SelectInstance<*> -> "select"
                    is ReceiveCatching<*> -> "receiveCatching"
                    is SendBroadcast -> "send(broadcast)"
                    is WaiterEB -> "EB($w)"
                    else -> w.toString()
                }
                val eString = e.toString()
                sb.append("[$i]=($wString,$eString),")
            }
            sb.append("next=${cur.next?.hexAddress}]  ")
            cur = cur.next ?: break
        }
        return sb.toString()
    }

    fun checkSegmentStructure() {
        if (isRendezvousOrUnlimited) {
            check(bufferEndSegment.value === NULL_SEGMENT) {
                "bufferEndSegment must be NULL_SEGMENT for rendezvous and unlimited channels; they do not manipulate it"
            }
        } else {
            // OK because of isEmpty
//            check(receiveSegment.value.id <= bufferEndSegment.value.id) {
//                "bufferEndSegment should not have lower id than receiveSegment"
//            }
        }
        val firstSegment = listOf(receiveSegment.value, sendSegment.value, bufferEndSegment.value)
            .filter { it !== NULL_SEGMENT }
            .minBy { it.id }
        check(firstSegment.prev == null) {
            "All processed segments should be unreachable from the data structure, but the `prev` link of the leftmost segment is non-null"
        }
        var segment = firstSegment
        while (segment.next != null) {
//            THIS IS POSSIBLE AFTER CLOSING
//            check(segment.next!!.prev !== null) {
//                "`segm.next.prev` is null"
//            }
            check(segment.next!!.prev == null || segment.next!!.prev === segment) {
                "The `segm.next.prev === segm` invariant is violated"
            }
            var interruptedOrClosed = 0
            for (i in 0 until SEGMENT_SIZE) {
                val state = segment.getState(i)
                when (segment.getState(i)) {
                    BUFFERED -> {
                        // TODO
                    }
                    is Waiter -> {
                        // TODO
                    }
                    INTERRUPTED_RCV, INTERRUPTED_SEND, CHANNEL_CLOSED -> {
                        check(segment.getElement(i) == null)
                        interruptedOrClosed++
                    }
                    is WaiterEB -> error("The cell state is WaiterEB, which is a special state to solve when race when " +
                        "the cell processing by expandBuffer() is covered by both send(e) and receive(); " +
                        "this state must be updated when both send(e) and receive() complete.")
                    S_RESUMING_EB, S_RESUMING_RCV -> error("") // TODO
                    IN_BUFFER -> error("") // TODO
                    POISONED, DONE_RCV -> {} // it's OK
                    else -> error("Unexpected segment cell state: $state")
                }
            }
            if (interruptedOrClosed == SEGMENT_SIZE) {
                check(segment === receiveSegment.value || segment === sendSegment.value || segment === bufferEndSegment.value) {
                    "logically removed segment is reachable"
                }
            }
            segment = segment.next!!
        }
    }
}

/**
 * The channel is represented as a list of segments, which simulates an infinite array.
 * Each segment has its own [id], which increase from the beginning. These [id]s help
 * to update [BufferedChannel.sendSegment], [BufferedChannel.receiveSegment],
 * and [BufferedChannel.bufferEndSegment] correctly.
 */
internal class ChannelSegment<E>(id: Long, prev: ChannelSegment<E>?, channel: BufferedChannel<E>?, pointers: Int) : Segment<ChannelSegment<E>>(id, prev, pointers) {
    private val _channel: BufferedChannel<E>? = channel
    val channel get() = _channel!! // always non-null except for `NULL_SEGMENT`

    private val data = atomicArrayOfNulls<Any?>(SEGMENT_SIZE * 2) // 2 registers per slot: state + element
    override val numberOfSlots: Int get() = SEGMENT_SIZE

    // ########################################
    // # Manipulation with the Element Fields #
    // ########################################

    internal fun storeElement(index: Int, element: E) {
        setElementLazy(index, element)
    }

    @Suppress("UNCHECKED_CAST")
    internal fun getElement(index: Int) = data[index * 2].value as E

    internal fun retrieveElement(index: Int): E = getElement(index).also { cleanElement(index) }

    internal fun cleanElement(index: Int) {
        setElementLazy(index, null)
    }

    private fun setElementLazy(index: Int, value: Any?) {
        data[index * 2].lazySet(value)
    }

    // ######################################
    // # Manipulation with the State Fields #
    // ######################################

    internal fun getState(index: Int): Any? = data[index * 2 + 1].value

    internal fun setState(index: Int, value: Any?) {
        data[index * 2 + 1].value = value
    }

    internal fun casState(index: Int, from: Any?, to: Any?) = data[index * 2 + 1].compareAndSet(from, to)

    internal fun getAndSetState(index: Int, update: Any?) = data[index * 2 + 1].getAndSet(update)


    // ########################
    // # Cancellation Support #
    // ########################

    fun onSenderCancellationWithOnUndeliveredElement(index: Int, context: CoroutineContext) {
        val element = getElement(index)
        if (onCancellationImpl(index)) {
            channel.onUndeliveredElement!!.callUndeliveredElement(element, context)
        }
    }

    fun onCancellation(index: Int) {
        onCancellationImpl(index)
    }

    @Suppress("ConvertTwoComparisonsToRangeCheck")

    private fun onCancellationImpl(index: Int): Boolean {
        // Update the cell state first.
        val globalIndex = id * SEGMENT_SIZE + index
        val s = channel.sendersCounter
        val r = channel.receiversCounter

        var isSender: Boolean
        var isReceiver: Boolean

        while (true) {
            val cur = data[index * 2 + 1].value
            when {
                cur is Waiter || cur is WaiterEB -> {
                    isSender = globalIndex < s && globalIndex >= r
                    isReceiver = globalIndex < r && globalIndex >= s
                    if (!isSender && !isReceiver) {
                        cleanElement(index)
                        return true
                    }
                    val update = if (isSender) INTERRUPTED_SEND else INTERRUPTED_RCV
                    if (data[index * 2 + 1].compareAndSet(cur, update)) break
                }

                cur === INTERRUPTED_SEND || cur === INTERRUPTED_RCV -> {
                    cleanElement(index)
                    return true
                }
                cur === S_RESUMING_EB || cur === S_RESUMING_RCV -> continue
                cur === DONE_RCV || cur === BUFFERED -> return false
                cur === CHANNEL_CLOSED -> {
                    return false
                }
                else -> error("unexpected state: $cur")
            }
        }
        cleanElement(index)
        onCancelledRequest(index, isReceiver)
        return true
    }

    fun onCancelledRequest(index: Int, receiver: Boolean) {
        if (receiver) channel.waitExpandBufferCompletion(id * SEGMENT_SIZE + index)
        onSlotCleaned()
    }
}
private fun <E> createSegment(id: Long, prev: ChannelSegment<E>) = ChannelSegment(
    id = id,
    prev = prev,
    channel = prev.channel,
    pointers = 0
)

private val NULL_SEGMENT = ChannelSegment<Any?>(id = -1, prev = null, channel = null, pointers = 0)

/**
 * Number of cells in each segment.
 */
private val SEGMENT_SIZE = systemProp("kotlinx.coroutines.bufferedChannel.segmentSize", 32)

private val EXPAND_BUFFER_COMPLETION_WAIT_ITERATIONS = systemProp("kotlinx.coroutines.bufferedChannel.expandBufferCompletionWaitIterations", 10_000)

/**
 * Tries to resume this continuation with the specified
 * value. Returns `true` on success and `false` on failure.
 */
private fun <T> CancellableContinuation<T>.tryResume0(
    value: T,
    onCancellation: ((cause: Throwable) -> Unit)? = null
): Boolean =
    tryResume(value, null, onCancellation).let { token ->
        if (token != null) {
            completeResume(token)
            true
        } else false
    }

/*
  When the channel is rendezvous or unlimited, the `bufferEnd` counter
  should be initialized with the corresponding value below and never change.
  In this case, the `expandBuffer(..)` operation does nothing.
 */
private const val BUFFER_END_RENDEZVOUS = 0L // no buffer
private const val BUFFER_END_UNLIMITED = Long.MAX_VALUE // infinite buffer
private fun initialBufferEnd(capacity: Int): Long = when (capacity) {
    Channel.RENDEZVOUS -> BUFFER_END_RENDEZVOUS
    Channel.UNLIMITED -> BUFFER_END_UNLIMITED
    else -> capacity.toLong()
}

/*
  Cell states. The initial "empty" state is represented with `null`,
  and suspended operations are represented with [Waiter] instances.
 */

// The cell stores a buffered element.
private val BUFFERED = Symbol("BUFFERED")
// Concurrent `expandBuffer(..)` can inform the
// upcoming sender that it should buffer the element.
private val IN_BUFFER = Symbol("SHOULD_BUFFER")
// Indicates that a receiver (R suffix) is resuming
// the suspended sender; after that, it should update
// the state to either `BUFFERED` (on success) or
// `INTERRUPTED_SEND` (on failure).
private val S_RESUMING_RCV = Symbol("RESUMING_R")
// Indicates that `expandBuffer(..)` is resuming the
// suspended sender; after that, it should update the
// state to either `BUFFERED` (on success) or
// `INTERRUPTED_SEND` (on failure).
private val S_RESUMING_EB = Symbol("RESUMING_EB")
// When a receiver comes to the cell already covered by
// a sender (according to the counters), but the cell
// is still in `EMPTY` or `IN_BUFFER` state, it breaks
// the cell by changing its state to `POISONED`.
private val POISONED = Symbol("POISONED")
// When the element is successfully transferred (possibly,
// through buffering) to a suspended receiver, the cell
// changes to `DONE_RCV`.
private val DONE_RCV = Symbol("DONE_RCV")
// Cancelled sender.
private val INTERRUPTED_SEND = Symbol("INTERRUPTED_SEND")
// Cancelled receiver.
private val INTERRUPTED_RCV = Symbol("INTERRUPTED_RCV")
// Indicates that the channel is closed.
internal val CHANNEL_CLOSED = Symbol("CHANNEL_CLOSED")
// When the cell is already covered by both sender and
// receiver (`sender` and `receivers` counters are greater
// than the cell number), the `expandBuffer(..)` procedure
// cannot distinguish which kind of operation is stored
// in the cell. Thus, it wraps the waiter with this descriptor,
// informing the possibly upcoming receiver that it should
// complete the `expandBuffer(..)` procedure if the waiter
// stored in the cell is sender. In turn, senders ignore this
// information.
private class WaiterEB(@JvmField val waiter: Waiter) {
    override fun toString() = "WaiterEB($waiter)"
}



/**
 * To distinguish suspended [BufferedChannel.receive] and
 * [BufferedChannel.receiveCatching] operation, the last
 * is wrapped with this class.
 */
private class ReceiveCatching<E>(
    @JvmField val cont: CancellableContinuation<ChannelResult<E>>
) : Waiter

/*
  Internal results for [BufferedChannel.updateCellReceive].
  On successful rendezvous with waiting sender or
  buffered element retrieval, the corresponding element
  is returned as result of [BufferedChannel.updateCellReceive].
 */
private val SUSPEND = Symbol("SUSPEND")
private val SUSPEND_NO_WAITER = Symbol("SUSPEND_NO_WAITER")
private val FAILED = Symbol("FAILED")

/*
  Internal results for [BufferedChannel.updateCellSend]
 */
private const val RESULT_RENDEZVOUS = 0
private const val RESULT_BUFFERED = 1
private const val RESULT_SUSPEND = 2
private const val RESULT_SUSPEND_NO_WAITER = 3
private const val RESULT_CLOSED = 4
private const val RESULT_FAILED = 5

/**
 * Special value for [BufferedChannel.BufferedChannelIterator.receiveResult]
 * that indicates the absence of pre-received result.
 */
private val NO_RECEIVE_RESULT = Symbol("NO_RECEIVE_RESULT")


/*
  The channel close statuses. The transition scheme is the following:
    +--------+   +----------------------+   +-----------+
    | ACTIVE |-->| CANCELLATION_STARTED |-->| CANCELLED |
    +--------+   +----------------------+   +-----------+
        |                                         ^
        |             +--------+                  |
        +------------>| CLOSED |------------------+
                      +--------+
  We need `CANCELLATION_STARTED` to synchronize
  concurrent closing and cancellation.
 */
private const val CLOSE_STATUS_ACTIVE = 0
private const val CLOSE_STATUS_CANCELLATION_STARTED = 1
private const val CLOSE_STATUS_CLOSED = 2
private const val CLOSE_STATUS_CANCELLED = 3

/*
  The `senders` counter and the channel close status
  are stored in a single 64-bit register to save the space
  and reduce the number of reads in sending operations.
  The code below encapsulates the required bit arithmetics.
 */
private const val CLOSE_STATUS_SHIFT = 60
private const val COUNTER_MASK = (1L shl CLOSE_STATUS_SHIFT) - 1
private inline val Long.counter get() = this and COUNTER_MASK
private inline val Long.closeStatus: Int get() = (this shr CLOSE_STATUS_SHIFT).toInt()
private fun constructSendersAndCloseStatus(counter: Long, closeStatus: Int): Long =
    (closeStatus.toLong() shl CLOSE_STATUS_SHIFT) + counter

/*
  As [BufferedChannel.invokeOnClose] can be invoked concurrently
  with channel closing, we have to synchronize them. These two
  markers help with the synchronization. The resulting
  [BufferedChannel.closeHandler] state diagram is presented below:

    +------+ install handler +---------+  close  +---------+
    | null |---------------->| handler |-------->| INVOKED |
    +------+                 +---------+         +---------+
       |             +--------+
       +------------>| CLOSED |
           close     +--------+
 */
private val CLOSE_HANDLER_CLOSED = Symbol("CLOSE_HANDLER_CLOSED")
private val CLOSE_HANDLER_INVOKED = Symbol("CLOSE_HANDLER_INVOKED")

/**
 * Specifies the absence of closing cause, stored in [BufferedChannel.closeCause].
 * When the channel is closed or cancelled without exception, this [NO_CLOSE_CAUSE]
 * marker should be replaced with `null`.
 */
private val NO_CLOSE_CAUSE = Symbol("NO_CLOSE_CAUSE")

/**
 * All waiters, such as [CancellableContinuationImpl], [SelectInstance], and
 * [BufferedChannel.BufferedChannelIterator], should be marked with this interface
 * to make the code faster and easier to read.
 */
internal interface Waiter
