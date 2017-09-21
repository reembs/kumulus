package org.oryx.kumulus

import org.apache.storm.tuple.MessageId
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.component.KumulusComponent
import org.oryx.kumulus.component.TupleImpl

class KumulusTuple(
        component: KumulusComponent,
        streamId: String,
        tuple: List<Any>,
        anchors: Collection<Tuple>?,
        messageId: Any?
) {
    val kTuple: Tuple = TupleImpl(component.context, tuple, component.taskId(), streamId, KumulusMessageId(anchors), messageId)

    override fun toString(): String {
        return "KumulusTuple: MsgID ${(kTuple as TupleImpl).spoutMessageId}, Source: ${kTuple.sourceComponent}, Source Stream: ${kTuple.sourceStreamId}, Tuple: ${kTuple.values}"
    }
}

class KumulusMessageId(anchors: Collection<Tuple>?) : MessageId(HashMap<Long, Long>())
