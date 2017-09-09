package org.oryx.kumulus.collector

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.oryx.kumulus.KumulusEmitter

abstract class KumulusCollector(
        private val componentRegisteredOutputs: List<Pair<String, Pair<String, Grouping>>>,
        private val emitter: KumulusEmitter
) {

    fun emit(streamId: String?, tuple: MutableList<Any>?) : MutableList<Int> {
        val outputPairs = componentRegisteredOutputs.filter { it.first == streamId }

        val ret = mutableListOf<Int>()
        outputPairs.forEach {
            ret += emitter.emit(GlobalStreamId(it.second.first, streamId), it.second.second, tuple)
        }

        return ret
    }
}
