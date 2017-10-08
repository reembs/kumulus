package org.oryx.kumulus.component

import mu.KotlinLogging
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichSpout
import org.oryx.kumulus.KumulusAcker
import org.oryx.kumulus.KumulusTopology
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

    val spout: IRichSpout = componentInstance

    private val deactivationLock = Any()
    private val deactivated = AtomicBoolean(false)

    val queue = LinkedBlockingQueue<AckMessage>()

    fun prepare(collector: KumulusSpoutCollector) {
        logger.debug { "Created spout '$componentId' with taskId $taskId (index: ${context.thisTaskIndex}). Object hashcode: ${this.hashCode()}" }
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
            this.isDaemon = true
            this.start()
        }
    }

    private fun mainLoopMethod(acker: KumulusAcker) {
        queue.poll()?.also { ackMessage ->
            if (ackMessage.ack) {
                spout.ack(ackMessage.spoutMessageId)
            } else {
                spout.fail(ackMessage.spoutMessageId)
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