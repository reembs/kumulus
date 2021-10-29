package org.xyro.kumulus

import mu.KotlinLogging
import org.apache.storm.shade.org.eclipse.jetty.util.ConcurrentHashSet
import org.apache.storm.tuple.Tuple
import org.xyro.kumulus.component.KumulusComponent
import org.xyro.kumulus.component.KumulusSpout
import org.xyro.kumulus.component.TupleImpl
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

class KumulusAcker(
    private val emitter: KumulusEmitter,
    private val maxSpoutPending: Long,
    private val allowExtraAcking: Boolean,
    private val messageTimeoutMillis: Long,
    private val spoutAvailabilityCheckTimeout: Long
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
        if (maxSpoutPending < 0) {
            throw IllegalArgumentException("maxSpoutPending = $maxSpoutPending")
        }
    }

    fun startTree(component: KumulusSpout, messageId: Any?) {
        logger.debug { "startTree() -> component: $component, messageId: $messageId" }
        if (messageId == null) {
            notifySpout(component, messageId, listOf()) {}
        } else {
            MessageState(component).let { messageState ->
                synchronized(completeLock) {
                    if (state[messageId] != null) {
                        logger.error { "messageId $messageId is currently being processes. Duplicate IDs are not allowed" }
                        throw RuntimeException("Duplicate ID: $messageId")
                    }
                    state[messageId] = messageState
                    var spoutAvailable: Boolean
                    do {
                        spoutAvailable = waitForSpoutAvailability()
                    } while (!spoutAvailable)
                    val currentPending = currentPending.incrementAndGet()
                    if (maxSpoutPending > 0) {
                        if (currentPending > maxSpoutPending) {
                            logger.error { "Exceeding max-spout-pending of $maxSpoutPending, current $currentPending" }
                            assert(false) {
                                "Exceeding max-spout-pending of $maxSpoutPending, current $currentPending"
                            }
                        }
                    }
                }

                if (messageTimeoutMillis > 0) {
                    timeoutExecutor.schedule(
                        {
                            state[messageId]?.let { messageState ->
                                val removedState = state.remove(messageId)
                                if (removedState != null) {
                                    notifySpout(
                                        messageState.spout,
                                        messageId,
                                        removedState.pendingTasks.map { it.key },
                                        removedState.failedTasks.toList()
                                    ) {
                                        decrementPending()
                                    }
                                }
                            }
                        },
                        messageTimeoutMillis, TimeUnit.MILLISECONDS
                    )
                }
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
                state[messageId]
                    ?: error("State missing for messageId $messageId while emitting from $component to $dest. Tuple: $tuple")
            messageState.pendingTasks.put(dest, tuple.kTuple)
        }
    }

    fun fail(component: KumulusComponent, input: Tuple?) {
        logger.debug { "fail() -> component: $component, input: $input" }
        (input as TupleImpl).spoutMessageId?.let { messageId ->
            val messageState = state[messageId]
            if (allowExtraAcking && messageState == null) {
                return
            }
            if (messageState == null) {
                error("State missing for messageId $messageId while failing tuple in $component. Tuple: $input")
            }
            messageState.failedTasks.add(component.taskId)
            checkComplete(messageState, component, input)
        }
    }

    fun ack(component: KumulusComponent, input: Tuple?) {
        logger.debug { "ack() -> component: $component, input: $input" }
        (input as TupleImpl).spoutMessageId?.let { messageId ->
            val messageState = state[messageId]
            if (allowExtraAcking && state[messageId] == null) {
                return
            }
            if (messageState == null)
                error("State missing for messageId $messageId while acking tuple in $component. Tuple: $input")
            checkComplete(messageState, component, input)
        }
    }

    fun waitForSpoutAvailability(): Boolean {
        if (maxSpoutPending > 0) {
            synchronized(waitObject) {
                if (currentPending.get() >= maxSpoutPending) {
                    logger.trace { "Waiting for spout availability" }
                    waitObject.wait(spoutAvailabilityCheckTimeout)
                }
            }
            return currentPending.get() < maxSpoutPending
        }
        return true
    }

    fun releaseSpoutBlocks() {
        synchronized(waitObject) {
            waitObject.notifyAll()
        }
    }

    fun getPendingCount(): Long {
        return this.currentPending.get()
    }

    private fun checkComplete(messageState: MessageState, component: KumulusComponent, input: Tuple?) {
        (input as TupleImpl).spoutMessageId?.let { spoutMessageId ->
            val removedTask = messageState.pendingTasks.remove(component.taskId)
            if (removedTask == null) {
                logger.debug {
                    "Key $spoutMessageId / ${component.taskId} was not found in execution map for $component"
                }
            } else {
                debugMessage(component, spoutMessageId, messageState)
                if (messageState.pendingTasks.isEmpty()) {
                    logger.debug {
                        "[${component.componentId}/${component.taskId}] " +
                            "Finished with messageId $spoutMessageId"
                    }
                    val removedState = state.remove(spoutMessageId)
                    if (removedState != null) {
                        notifySpout(messageState.spout, spoutMessageId, messageState.failedTasks.toList()) {
                            decrementPending()
                        }
                    } else {
                        logger.debug { "Race while closing tuple-tree, ignoring duplicate" }
                    }
                }
            }
        }
    }

    private fun debugMessage(component: KumulusComponent, spoutMessageId: Any, messageState: MessageState) {
        logger.debug {
            "Pending task from $component for message $spoutMessageId was completed. " +
                "Current pending tuples are:" + messageState.pendingTasks.let {
                if (it.isEmpty()) {
                    " Empty\n"
                } else {
                    val sb = StringBuilder("\n")
                    it.forEach { (k, v) ->
                        sb.append("$k: $v\n")
                    }
                    sb.toString()
                }
            }
        }
    }

    private fun notifySpout(spout: KumulusSpout, spoutMessageId: Any?, failedTasks: List<Int>, callback: () -> Unit) {
        this.notifySpout(spout, spoutMessageId, listOf(), failedTasks, callback)
    }

    private fun notifySpout(
        spout: KumulusSpout,
        spoutMessageId: Any?,
        timeoutTasks: List<Int>,
        failedTasks: List<Int>,
        callback: () -> Unit
    ) {
        emitter.completeMessageProcessing(spout, spoutMessageId, timeoutTasks, failedTasks, callback)
    }

    private fun decrementPending() {
        if (maxSpoutPending > 0) {
            synchronized(waitObject) {
                val currentPending = currentPending.decrementAndGet()
                if (currentPending >= maxSpoutPending) {
                    logger.error { "Max spout pending must have exceeded limit of $maxSpoutPending, current after decrement is $currentPending" }
                    assert(false) {
                        "Max spout pending must have exceeded limit of $maxSpoutPending, current after decrement is $currentPending"
                    }
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
        val pendingTasks = ConcurrentHashMap<Int, Tuple>()
        val failedTasks = ConcurrentHashSet<Int>()
    }
}
