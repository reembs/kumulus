package org.xyro.kumulus.component

import mu.KotlinLogging
import org.apache.storm.generated.ComponentCommon
import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.xyro.kumulus.KumulusTuple
import org.xyro.kumulus.collector.KumulusBoltCollector

class KumulusBolt(
        config: Map<String, Any>,
        context: TopologyContext,
        componentInstance: IRichBolt,
        common: ComponentCommon?
) : KumulusComponent(config, context) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    val inputs: Map<GlobalStreamId, Grouping> = common?._inputs?.toMap() ?: mapOf()
    val streams = common?._streams?.toMap() ?: mapOf()

    var tickSecs: Number? = null

    private val bolt : IRichBolt = componentInstance

    fun prepare(collector: KumulusBoltCollector) {
        logger.debug { "Created bolt '$componentId' with taskId $taskId (index: ${context.thisTaskIndex}). Object hashcode: ${this.hashCode()}" }
        bolt.prepare(config, context, OutputCollector(collector))
        super.prepare()
    }

    fun execute(tuple: KumulusTuple) {
        logger.debug { "Executing bolt '$componentId' with taskId $taskId (index: ${context.thisTaskIndex}). Input: ${tuple.kTuple}" }
        bolt.execute(tuple.kTuple)
    }
}