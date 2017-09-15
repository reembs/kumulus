package org.oryx.kumulus

import org.apache.storm.tuple.MessageId
import org.apache.storm.tuple.Tuple
import org.apache.storm.tuple.TupleImpl
import org.oryx.kumulus.component.KumulusComponent

class KumulusTuple(component: KumulusComponent, streamId: String, tuple: List<Any>, anchors: Collection<Tuple>?) {
    val kTuple: Tuple = TupleImpl(component.context, tuple, component.taskId(), streamId, KumulusMessageId(anchors))
}

class KumulusMessageId(anchors: Collection<Tuple>?) : MessageId(HashMap<Long, Long>())
