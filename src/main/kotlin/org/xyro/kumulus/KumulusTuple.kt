package org.xyro.kumulus

import org.apache.storm.tuple.MessageId
import org.apache.storm.tuple.Tuple
import org.xyro.kumulus.component.KumulusComponent
import org.xyro.kumulus.component.TupleImpl

class KumulusTuple(
        component: KumulusComponent,
        streamId: String,
        tuple: List<Any>,
        messageId: Any?
) {
    private val spoutMessageId = messageId

    val kTuple: Tuple = TupleImpl(
            component.context,
            tuple,
            component.taskId,
            streamId,
            KumulusMessageId(),
            spoutMessageId
    )

    override fun toString(): String {
        return "KumulusTuple: " +
                "MsgID ${(kTuple as TupleImpl).spoutMessageId}, " +
                "Source: ${kTuple.sourceComponent}, " +
                "Source Stream: ${kTuple.sourceStreamId}, " +
                "Tuple: ${kTuple.values}"
    }

    class KumulusMessageId : MessageId(HashMap<Long, Long>())
}
