package org.oryx.kumulus

import org.apache.storm.Config
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichSpout

class KumulusSpout(
        config: Config,
        context: TopologyContext,
        componentInstance: IRichSpout
) : KumulusComponent(config, context, componentInstance) {

    private val spout: IRichSpout = componentInstance

    override fun prepare() {
        println("Created ${context.thisComponentId} with taskId ${context.thisTaskId} (index: ${context.thisTaskIndex}). Object hashcode: ${this.hashCode()}")
        spout.open(config, context, null)
    }
}