package org.xyro.kumulus.component

import mu.KotlinLogging
import org.apache.storm.Config
import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.generated.StreamInfo
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.xyro.kumulus.KumulusTopologyContext
import org.xyro.kumulus.KumulusTuple
import org.xyro.kumulus.collector.KumulusBoltCollector

class KumulusBolt(
        config: Map<String, Any>,
        componentId: String,
        taskId: Int,
        private val bolt : IRichBolt,
        val inputs: Map<GlobalStreamId, Grouping>,
        val outputs: Map<String, StreamInfo>
) : KumulusComponent(config, componentId, taskId) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    var tickSecs: Number? = null

    fun prepare(collector: KumulusBoltCollector) {
        logger.info { "Created bolt '$componentId' with taskId $taskId (index: ${getThisTaskIndex()}). Object hashcode: ${this.hashCode()}" }

        super.prepare()

        if (bolt is KumulusIRichBolt) {
            bolt.prepare(config, kContext as KumulusTopologyContext, OutputCollector(collector))
        } else {
            bolt.prepare(config, context as TopologyContext, OutputCollector(collector))
        }
    }

    fun execute(tuple: KumulusTuple) {
        logger.debug { "Executing bolt '$componentId' with taskId $taskId (index: ${getThisTaskIndex()}). Input: ${tuple.kTuple}" }
        bolt.execute(tuple.kTuple)
    }

    init {
        (bolt.componentConfiguration ?: mapOf())[Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS]?.let { secs ->
            assert(secs is Number)
            (secs as Number).let {
                this.tickSecs = secs
            }
        }
    }
}