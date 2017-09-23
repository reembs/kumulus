package org.oryx.kumulus.component

import mu.KotlinLogging
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichSpout
import org.oryx.kumulus.collector.KumulusSpoutCollector
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

class KumulusSpout(
        config: Map<String, Any>,
        context: TopologyContext,
        componentInstance: IRichSpout
) : KumulusComponent(config, context) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    private val spout: IRichSpout = componentInstance
    private val deactivationLock = Any()
    private val deactivated = AtomicBoolean(false)

    val queue = LinkedBlockingQueue<AckMessage>()

    fun prepare(collector: KumulusSpoutCollector) {
        logger.debug { "Created spout '${context.thisComponentId}' with taskId ${context.thisTaskId} (index: ${context.thisTaskIndex}). Object hashcode: ${this.hashCode()}" }
        spout.open(config, context, SpoutOutputCollector(collector))
        super.prepare()
    }

    fun nextTuple() {
        spout.nextTuple()
    }

    fun ack(msgId: Any?) {
        spout.ack(msgId)
    }

    fun fail(msgId: Any?) {
        spout.fail(msgId)
    }

    fun activate() {
        spout.activate()
    }

    fun deactivate() {
        if (!deactivated.get()) {
            synchronized(deactivationLock) {
                if (!deactivated.get()) {
                    deactivated.set(true)
                    spout.deactivate()
                }
            }
        }
    }
}