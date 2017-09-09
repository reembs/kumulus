package org.oryx.kumulus

import org.apache.storm.Config
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt

class KumulusBolt(
        config: Config,
        context: TopologyContext,
        componentInstance: IRichBolt
) : KumulusComponent(config, context, componentInstance) {

    private val bolt : IRichBolt = componentInstance

    override fun prepare() {
        println("Created ${context.thisComponentId} with taskId ${context.thisTaskId} (index: ${context.thisTaskIndex}). Object hashcode: ${this.hashCode()}")
        bolt.prepare(config, context, null)
    }
}