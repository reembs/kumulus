package org.oryx.kumulus.component

import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.collector.KumulusBoltCollector

class KumulusBolt(
        config: MutableMap<String, Any>,
        context: TopologyContext,
        componentInstance: IRichBolt
) : KumulusComponent(config, context) {
    private val bolt : IRichBolt = componentInstance

    fun prepare(collector: KumulusBoltCollector) {
        println("Created bolt '${context.thisComponentId}' with taskId ${context.thisTaskId} (index: ${context.thisTaskIndex}). Object hashcode: ${this.hashCode()}")
        bolt.prepare(config, context, OutputCollector(collector))
    }

    fun execute(tuple: Tuple) {
        bolt.execute(tuple)
    }
}