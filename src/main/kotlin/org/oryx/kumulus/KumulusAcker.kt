package org.oryx.kumulus

import mu.KotlinLogging
import org.apache.storm.shade.org.eclipse.jetty.util.ConcurrentHashSet
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.component.KumulusComponent
import org.oryx.kumulus.component.KumulusSpout
import org.oryx.kumulus.component.TupleImpl
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

class KumulusAcker(private val emitter: KumulusEmitter) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val state = ConcurrentHashMap<Any, MessageState>()

    fun startTree(component: KumulusSpout, messageId: Any?) {
        val messageState = MessageState(component)
        state[messageId!!] = messageState
    }

    fun expandTrees(component: KumulusComponent, dest: Int, tuple: KumulusTuple) {
        logger.debug { "component: $component, dest: $dest, tuple: $tuple" }
        val messageId = (tuple.kTuple as TupleImpl).spoutMessageId
        val state = state[messageId]!!
        state.pendingTasks.add(Pair(dest, tuple.kTuple))
    }

    fun fail(component: KumulusComponent, input: Tuple?) {
        (input as TupleImpl).spoutMessageId?.let { messageId ->
            val messageState = state[messageId]!!
            messageState.ack.compareAndSet(true, false)
            checkComplete(messageState, component, input, false)
        }
    }

    fun ack(component: KumulusComponent, input: Tuple?) {
        (input as TupleImpl).spoutMessageId?.let { messageId ->
            val messageState = state[messageId]!!
            checkComplete(messageState, component, input, true)
        }
    }

    private fun checkComplete(messageState: MessageState, component: KumulusComponent, input: Tuple?, ack: Boolean) {
        val key = Pair(component.taskId(), input)
        assert(messageState.pendingTasks.contains(key)) { "Input tuple $input was not present in pendingTasks as expected" }
        messageState.pendingTasks.remove(key)
        (input as TupleImpl).spoutMessageId?.let { spoutMessageId ->
            if (messageState.pendingTasks.isEmpty()) {
                logger.debug { "Finished with messageId $spoutMessageId" }
                state.remove(spoutMessageId)
                emitter.completeMessageProcessing(messageState.spout, spoutMessageId, messageState.ack.get())
            }
        }
    }

    inner class MessageState(
            val spout: KumulusSpout
    ) {
        val pendingTasks = ConcurrentHashSet<Pair<Int, Tuple>>()
        var ack = AtomicBoolean(true)
    }
}