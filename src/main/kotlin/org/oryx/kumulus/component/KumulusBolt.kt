package org.oryx.kumulus.component

import mu.KotlinLogging
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.oryx.kumulus.KumulusTuple
import org.oryx.kumulus.collector.KumulusBoltCollector

class KumulusBolt(
        config: MutableMap<String, Any>,
        context: TopologyContext,
        componentInstance: IRichBolt
) : KumulusComponent(config, context) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val bolt : IRichBolt = componentInstance

    fun prepare(collector: KumulusBoltCollector) {
        logger.info { "Created bolt '${context.thisComponentId}' with taskId ${context.thisTaskId} (index: ${context.thisTaskIndex}). Object hashcode: ${this.hashCode()}" }
        bolt.prepare(config, context, OutputCollector(collector))
        super.prepare()
        inUse.set(false)
    }

    fun execute(tuple: KumulusTuple) {
        bolt.execute(tuple.kTuple)
    }
}