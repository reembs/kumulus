package org.oryx.kumulus.collector

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.oryx.kumulus.KumulusEmitter

abstract class KumulusCollector(
        private val componentRegisteredOutputs: Map<String, Pair<String, Grouping>>,
        private val emitter: KumulusEmitter
) {

    fun emit(streamId: String?, tuple: MutableList<Any>?) : MutableList<Int> {
        val outputPair = componentRegisteredOutputs.get(streamId) ?: throw IllegalArgumentException()
        return emitter.emit(GlobalStreamId(outputPair.first, streamId), outputPair.second, tuple)
    }
}
