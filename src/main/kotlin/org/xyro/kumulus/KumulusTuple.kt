package org.xyro.kumulus

import io.opentelemetry.context.Context
import org.apache.storm.tuple.MessageId
import org.apache.storm.tuple.Tuple
import org.xyro.kumulus.component.KumulusComponent
import org.xyro.kumulus.component.TupleImpl

class KumulusTuple(
    component: KumulusComponent,
    streamId: String,
    tuple: List<Any>,
    messageId: Any?,
    val loggingContext: Map<String, String> = emptyMap(),
    val otelContext: Context = Context.root(),
) {
    private val spoutMessageId = messageId

    val kTuple: Tuple =
        TupleImpl(
            component.context,
            tuple,
            component.taskId,
            streamId,
            KumulusMessageId(),
            spoutMessageId,
        )

    override fun toString(): String =
        "KumulusTuple: " +
            "MsgID ${(kTuple as TupleImpl).spoutMessageId}, " +
            "Source: ${kTuple.sourceComponent}, " +
            "Source Stream: ${kTuple.sourceStreamId}, " +
            "Tuple: ${kTuple.values}"

    class KumulusMessageId : MessageId(HashMap<Long, Long>())
}
