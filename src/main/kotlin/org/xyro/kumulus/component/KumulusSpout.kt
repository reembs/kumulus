package org.xyro.kumulus.component

import mu.KotlinLogging
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichSpout
import org.xyro.kumulus.KumulusAcker
import org.xyro.kumulus.KumulusTopology
import org.xyro.kumulus.KumulusTopologyContext
import org.xyro.kumulus.collector.KumulusSpoutCollector
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

class KumulusSpout(
        config: Map<String, Any>,
        componentId: String,
        taskId: Int,
        componentInstance: IRichSpout
) : KumulusComponent(config, componentId, taskId) {
    companion object {
        private val logger = KotlinLogging.logger {}
    }

    val spout: IRichSpout = componentInstance

    private val deactivationLock = Any()
    private val deactivated = AtomicBoolean(false)

    val queue = LinkedBlockingQueue<AckMessage>()

    fun prepare(collector: KumulusSpoutCollector) {
        logger.debug { "Created spout '$componentId' with taskId $taskId (index: ${getThisTaskIndex()}). Object hashcode: ${this.hashCode()}" }
        if (spout is KumulusIRichSpout) {
            spout.open(config, context as KumulusTopologyContext, SpoutOutputCollector(collector))
        } else {
            spout.open(config, context as TopologyContext, SpoutOutputCollector(collector))
        }
        super.prepare()
    }

    private fun nextTuple() {
        spout.nextTuple()
    }

    private fun ack(msgId: Any?) {
        spout.ack(msgId)
    }

    private fun fail(msgId: Any?) {
        spout.fail(msgId)
    }

    private fun activate() {
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

    fun start(topology: KumulusTopology) {
        Thread {
            try {
                while (true) {
                    if (isReady.get()) {
                        activate()
                        break
                    }
                    Thread.sleep(topology.busyPollSleepTime)
                }
                while (true) {
                    mainLoopMethod(topology.acker)
                    if (!isReady.get()) {
                        spout.deactivate()
                        return@Thread
                    }
                }
            } catch (e: Exception) {
                logger.error("An uncaught exception in spout '$componentId' (taskId: $taskId) has forced a Kumulus shutdown", e)
                spout.deactivate()
                topology.stop()
                throw e
            }
        }.apply {
            isDaemon = true
            start()
        }
    }

    private fun mainLoopMethod(acker: KumulusAcker) {
        queue.poll()?.also { ackMessage ->
            if (ackMessage.ack) {
                ack(ackMessage.spoutMessageId)
            } else {
                fail(ackMessage.spoutMessageId)
            }
        }.let {
            if (it == null && isReady.get()) {
                acker.waitForSpoutAvailability()
                if (inUse.compareAndSet(false, true)) {
                    try {
                        if (isReady.get()) {
                            nextTuple()
                        }
                    } finally {
                        inUse.set(false)
                    }
                }
            }
        }
    }
}