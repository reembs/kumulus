package org.xyro.kumulus.component

import mu.KotlinLogging
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichSpout
import org.xyro.kumulus.KumulusAcker
import org.xyro.kumulus.KumulusTopology
import org.xyro.kumulus.collector.KumulusSpoutCollector
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

    private fun nextTuple() {
        spout.nextTuple()
    }

    private fun ack(msgId: Any?) {
        spout.ack(msgId)
    }

    private fun fail(
            msgId: Any?,
            timeoutComponents: List<String>,
            failedComponents: List<String>
    ) {
        if (spout is KumulusTimeoutNotificationSpout) {
            spout.messageIdFailure(msgId, failedComponents, timeoutComponents)
        } else if (spout is KumulusFailureNotificationSpout) {
            spout.messageIdFailure(msgId, failedComponents + timeoutComponents)
        }
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
                    Thread.sleep(topology.readyPollSleepTime)
                }
                while (true) {
                    mainLoopMethod(topology.acker)
                    if (!isReady.get()) {
                        spout.deactivate()
                        return@Thread
                    }
                }
            } catch (e: Exception) {
                logger.error("An uncaught exception in spout '$componentId' (taskId: $taskId) had forced a Kumulus shutdown", e)
                topology.throwException(e)
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
                fail(ackMessage.spoutMessageId, ackMessage.timeoutComponents, ackMessage.failedComponents)
            }
            ackMessage.callback()
        }.let {
            if (it == null && isReady.get()) {
                if (acker.waitForSpoutAvailability()) {
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
}