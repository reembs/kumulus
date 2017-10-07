package org.oryx.kumulus

import mu.KotlinLogging
import org.apache.storm.shade.org.eclipse.jetty.util.ConcurrentHashSet
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.component.KumulusComponent
import org.oryx.kumulus.component.KumulusSpout
import org.oryx.kumulus.component.TupleImpl
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class KumulusAcker(
        private val emitter: KumulusEmitter,
        private val maxSpoutPending: Long,
        private val allowExtraAcking: Boolean,
        private val messageTimeoutMillis: Long
) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val state = ConcurrentHashMap<Any, MessageState>()
    private val waitObject = Object()
    private val currentPending = AtomicLong(0)
    private val completeLock = Any()
    private val timeoutExecutor = ScheduledThreadPoolExecutor(1)

    init {
        assert(maxSpoutPending >= 0)
    }

    fun startTree(component: KumulusSpout, messageId: Any?) {
        logger.debug { "startTree() -> component: $component, messageId: $messageId" }
        if (messageId == null) {
            notifySpout(component, messageId, true)
        } else {
            MessageState(component).let { messageState ->
                synchronized(completeLock) {
                    assert(state[messageId] == null) {
                        "messageId $messageId is currently being processes. Duplicate IDs are not allowed"
                    }
                    state[messageId] = messageState
                }

                if (messageTimeoutMillis > 0) {
                    timeoutExecutor.schedule({
                        if (state[messageId] == messageState) {
                            messageState.pendingTasks.map { it.second }
                            messageState.ack.compareAndSet(true, false)
                            forceComplete(messageId)
                        }
                    }, messageTimeoutMillis, TimeUnit.MILLISECONDS)
                }

                val currentPending = currentPending.incrementAndGet()
                if (maxSpoutPending > 0)
                    assert(currentPending <= maxSpoutPending) { "Exceeding max-spout-pending" }
            }
        }
    }

    fun expandTrees(component: KumulusComponent, dest: Int, tuple: KumulusTuple) {
        logger.debug { "expandTrees() -> component: $component, dest: $dest, tuple: $tuple" }
        (tuple.kTuple as TupleImpl).spoutMessageId?.let { messageId ->
            if (allowExtraAcking && state[messageId] == null) {
                return
            }
            val messageState =
                    state[messageId] ?:
                            error("State missing for messageId $messageId while emitting from $component to $dest. Tuple: $tuple")
            messageState.pendingTasks.add(Pair(dest, tuple.kTuple))
        }
    }

    fun fail(component: KumulusComponent, input: Tuple?) {
        logger.debug { "fail() -> component: $component, input: $input" }
        (input as TupleImpl).spoutMessageId?.let { messageId ->
            if (allowExtraAcking && state[messageId] == null) {
                return
            }
            val messageState =
                    state[messageId] ?:
                            error("State missing for messageId $messageId while failing tuple in $component. Tuple: $input")
            messageState.ack.compareAndSet(true, false)
            checkComplete(messageState, component, input)
        }
    }

    fun ack(component: KumulusComponent, input: Tuple?) {
        logger.debug { "ack() -> component: $component, input: $input" }
        (input as TupleImpl).spoutMessageId?.let { messageId ->
            if (allowExtraAcking && state[messageId] == null) {
                return
            }
            val messageState =
                    state[messageId] ?:
                            error("State missing for messageId $messageId while acking tuple in $component. Tuple: $input")
            checkComplete(messageState, component, input)
        }
    }

    fun waitForSpoutAvailability() {
        if (maxSpoutPending > 0) {
            synchronized(waitObject) {
                if (currentPending.get() >= maxSpoutPending) {
                    logger.trace { "Waiting for spout availability" }
                    waitObject.wait()
                }
            }
        }
    }

    fun releaseSpoutBlocks() {
        synchronized(waitObject) {
            waitObject.notifyAll()
        }
    }

    fun getPendingCount(): Long {
        return this.currentPending.get()
    }

    private fun forceComplete(spoutMessageId: Any) {
        state[spoutMessageId]?.let { messageState ->
            synchronized(completeLock) {
                state.remove(spoutMessageId)
            }?.let {
                notifySpout(messageState.spout, spoutMessageId, messageState.ack.get())
                decrementPending()
            }
        }
    }

    private fun checkComplete(messageState: MessageState, component: KumulusComponent, input: Tuple?) {
        val key = Pair(component.taskId(), input)
        (input as TupleImpl).spoutMessageId?.let { spoutMessageId ->
            if (synchronized(completeLock) {
                assert(messageState.pendingTasks.remove(key)) {
                    "Key $key was not found in execution map for $component" }
                logger.debug { "Pending task from $component for message $spoutMessageId was completed. " +
                        "Current pending tuples are:" + messageState.pendingTasks.let {
                    if (it.isEmpty()) {
                        " Empty\n"
                    } else {
                        val sb = StringBuilder("\n")
                        it.forEach {
                            sb.append("${it.first}: ${it.second}\n")
                        }
                        sb.toString()
                    }
                }}
                messageState.pendingTasks.isEmpty()
            }) {
                logger.debug { "[${component.context.thisComponentId}/${component.context.thisTaskId}] " +
                            "Finished with messageId $spoutMessageId" }
                state.remove(spoutMessageId)
                notifySpout(messageState.spout, spoutMessageId, messageState.ack.get())
                decrementPending()
            }
        }
    }

    private fun notifySpout(spout: KumulusSpout, spoutMessageId: Any?, ack: Boolean) {
        emitter.completeMessageProcessing(spout, spoutMessageId, ack)
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