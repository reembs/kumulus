package org.xyro.kumulus.component

import mu.KotlinLogging
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.xyro.kumulus.KumulusTuple
import org.xyro.kumulus.collector.KumulusBoltCollector

class KumulusBolt(
        config: Map<String, Any>,
        context: TopologyContext,
        componentInstance: IRichBolt
) : KumulusComponent(config, context) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    var tickSecs: Number? = null

    val bolt : IRichBolt = componentInstance

    fun prepare(collector: KumulusBoltCollector) {
        logger.info { "Created bolt '$componentId' with taskId $taskId (index: ${context.thisTaskIndex}). Object hashcode: ${this.hashCode()}" }
        bolt.prepare(config, context, OutputCollector(collector))
        super.prepare()
    }

    fun execute(tuple: KumulusTuple) {
        logger.debug { "Executing bolt '$componentId' with taskId $taskId (index: ${context.thisTaskIndex}). Input: ${tuple.kTuple}" }
        bolt.execute(tuple.kTuple)
    }
}