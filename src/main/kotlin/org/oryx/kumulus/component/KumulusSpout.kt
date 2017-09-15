package org.oryx.kumulus.component

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichSpout
import org.oryx.kumulus.collector.KumulusSpoutCollector

class KumulusSpout(
        config: MutableMap<String, Any>,
        context: TopologyContext,
        componentInstance: IRichSpout
) : KumulusComponent(config, context) {

    private val spout: IRichSpout = componentInstance

    fun prepare(collector: KumulusSpoutCollector) {
        println("Created spout '${context.thisComponentId}' with taskId ${context.thisTaskId} (index: ${context.thisTaskIndex}). Object hashcode: ${this.hashCode()}")
        spout.open(config, context, SpoutOutputCollector(collector))
        super.prepare()
    }

    fun nextTuple() {
        spout.nextTuple()
    }
}