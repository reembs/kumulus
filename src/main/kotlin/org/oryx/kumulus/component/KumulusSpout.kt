package org.oryx.kumulus.component

import mu.KotlinLogging
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichSpout
import org.oryx.kumulus.collector.KumulusSpoutCollector
import java.util.concurrent.LinkedBlockingDeque

class KumulusSpout(
        config: MutableMap<String, Any>,
        context: TopologyContext,
        componentInstance: IRichSpout
) : KumulusComponent(config, context) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val spout: IRichSpout = componentInstance

    val queue = LinkedBlockingDeque<AckMessage>()

    fun prepare(collector: KumulusSpoutCollector) {
        logger.info { "Created spout '${context.thisComponentId}' with taskId ${context.thisTaskId} (index: ${context.thisTaskIndex}). Object hashcode: ${this.hashCode()}" }
        spout.open(config, context, SpoutOutputCollector(collector))
        super.prepare()
        inUse.set(false)
    }

    fun nextTuple() {
        try {
            queue.poll()?.let { ackMsg ->
                if (ackMsg.ack) {
                    spout.ack(ackMsg.spoutMessageId)
                } else {
                    spout.fail(ackMsg.spoutMessageId)
                }
            }
            spout.nextTuple()
        } finally {
            inUse.set(false)
        }
    }
}