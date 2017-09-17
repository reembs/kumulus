package org.oryx.kumulus

import mu.KotlinLogging
import org.apache.storm.shade.org.eclipse.jetty.util.ConcurrentHashSet
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.component.KumulusComponent
import org.oryx.kumulus.component.KumulusSpout
import org.oryx.kumulus.component.TupleImpl
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class KumulusAcker(
        private val emitter: KumulusEmitter,
        private val maxSpoutPending: Int
) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val state = ConcurrentHashMap<Any, MessageState>()
    private val waitObject = Object()
    private val currentPending = AtomicInteger(0)
    private val completeLock = Any()

    init {
        assert(maxSpoutPending >= 0)
    }

    fun startTree(component: KumulusSpout, messageId: Any?) {
        logger.debug { "startTree() -> component: $component, messageId: $messageId" }
        val messageState = MessageState(component)
        assert(state[messageId!!] == null) {
            "messageId $messageId is currently being processes. Duplicate IDs are not allowed" }
        state[messageId] = messageState
        val currentPending = currentPending.incrementAndGet()
        if (maxSpoutPending > 0)
            assert(currentPending <= maxSpoutPending) { "Exceeding max-spout-pending" }
    }

    fun expandTrees(component: KumulusComponent, dest: Int, tuple: KumulusTuple) {
        val messageId = (tuple.kTuple as TupleImpl).spoutMessageId
        logger.debug { "expandTrees() -> component: $component, dest: $dest, tuple: $tuple, messageId: $messageId" }
        val state = state[messageId]!!
        state.pendingTasks.add(Pair(dest, tuple.kTuple))
    }

    fun fail(component: KumulusComponent, input: Tuple?) {
        (input as TupleImpl).spoutMessageId?.let { messageId ->
            logger.debug { "fail() -> component: $component, input: $input, messageId: $messageId" }
            val messageState = state[messageId]!!
            messageState.ack.compareAndSet(true, false)
            checkComplete(messageState, component, input)
        }
    }

    fun ack(component: KumulusComponent, input: Tuple?) {
        (input as TupleImpl).spoutMessageId?.let { messageId ->
            logger.debug { "ack() -> component: $component, input: $input, messageId: $messageId" }
            val messageState = state[messageId]!!
            checkComplete(messageState, component, input)
        }
    }

    fun waitForSpoutAvailability() {
        if (maxSpoutPending > 0) {
            synchronized(waitObject) {
                if (currentPending.get() >= maxSpoutPending) {
                    logger.debug { "Waiting for spout availability" }
                    waitObject.wait()
                }
            }
        }
    }

    private fun checkComplete(messageState: MessageState, component: KumulusComponent, input: Tuple?) {
        val key = Pair(component.taskId(), input)
        (input as TupleImpl).spoutMessageId?.let { spoutMessageId ->
            if (synchronized(completeLock) {
                assert(messageState.pendingTasks.remove(key)) {
                    "Key $key was not found in execution map for $component" }
                messageState.pendingTasks.isEmpty()
            }) {
                logger.debug { "[${component.context.thisComponentId}/${component.context.thisTaskId}] " +
                            "Finished with messageId $spoutMessageId" }
                state.remove(spoutMessageId)
                emitter.completeMessageProcessing(messageState.spout, spoutMessageId, messageState.ack.get())
                decrementPending()
            }
        }
    }

    private fun decrementPending() {
        if (maxSpoutPending > 0) {
            synchronized(waitObject) {
                val currentPending = currentPending.decrementAndGet()
                assert(currentPending < maxSpoutPending) {
                    "Max spout pending must have exceeded limit of $maxSpoutPending, current after decrement is $currentPending"
                }
                if (currentPending == maxSpoutPending - 1) {
                    waitObject.notify()
                }
            }
        } else {
            currentPending.decrementAndGet()
        }
    }

    inner class MessageState(
            val spout: KumulusSpout
    ) {
        val pendingTasks = ConcurrentHashSet<Pair<Int, Tuple>>()
        var ack = AtomicBoolean(true)
    }
}