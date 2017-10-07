package org.oryx.kumulus

import org.apache.storm.tuple.MessageId
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.component.KumulusComponent
import org.oryx.kumulus.component.TupleImpl

class KumulusTuple(
        component: KumulusComponent,
        streamId: String,
        tuple: List<Any>,
        val messageId: Any?
) {
    val spoutMessageId = messageId

    val kTuple: Tuple = TupleImpl(component.context, tuple, component.taskId, streamId, KumulusMessageId(), spoutMessageId)

    override fun toString(): String {
        return "KumulusTuple: MsgID ${(kTuple as TupleImpl).spoutMessageId}, Source: ${kTuple.sourceComponent}, Source Stream: ${kTuple.sourceStreamId}, Tuple: ${kTuple.values}"
    }

    class KumulusMessageId : MessageId(HashMap<Long, Long>())
}
