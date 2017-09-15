package org.oryx.kumulus.collector

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.KumulusAcker
import org.oryx.kumulus.KumulusEmitter
import org.oryx.kumulus.component.KumulusComponent

abstract class KumulusCollector<T: KumulusComponent>(
        protected val component : KumulusComponent,
        private val componentRegisteredOutputs: List<Pair<String, Pair<String, Grouping>>>,
        private val emitter: KumulusEmitter,
        protected val acker : KumulusAcker
) {
    private fun emit(streamId: String?, tuple: MutableList<Any>?, anchors: Collection<Tuple>?) : MutableList<Int> {
        val outputPairs = componentRegisteredOutputs.filter { it.first == streamId }

        val ret = mutableListOf<Int>()
        outputPairs.forEach {
            ret += emitter.emit(component, GlobalStreamId(it.second.first, streamId), it.second.second, tuple!!, anchors)
        }

        return ret
    }

    fun emit(streamId: String?, anchors: MutableCollection<Tuple>?, tuple: MutableList<Any>?): MutableList<Int> {
        acker.expandTrees(anchors)
        return emit(streamId, tuple, anchors)
    }

    fun emit(streamId: String?, tuple: MutableList<Any>?, messageId: Any?): MutableList<Int> {
        acker.startTree(messageId)
        return emit(streamId, tuple, null)
    }
}
