package org.oryx.kumulus

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.component.KumulusComponent
import java.util.concurrent.ConcurrentHashMap

class KumulusAcker {
    private val state = ConcurrentHashMap<Any, MessageState>()

    fun startTree(component: KumulusComponent, messageId: Any?) {
        state[messageId!!] = MessageState(messageId, component)
    }

    fun expandTrees(component: KumulusComponent, dest: GlobalStreamId, anchors: Collection<Tuple>?) {

    }

    fun fail(component: KumulusComponent, input: Tuple?) {
        val messageId = input?.messageId

    }

    fun ack(component: KumulusComponent, input: Tuple?) {
        val messageId = input?.messageId
    }

    inner class MessageState(
            val messageId: Any,
            val component: KumulusComponent
    )
}