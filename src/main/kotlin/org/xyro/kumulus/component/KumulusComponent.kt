package org.xyro.kumulus.component

import mu.KotlinLogging
import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.grouping.CustomStreamGrouping
import org.apache.storm.grouping.ShuffleGrouping
import org.apache.storm.task.TopologyContext
import org.apache.storm.tuple.Tuple
import org.apache.storm.utils.Utils
import org.jctools.queues.atomic.MpscLinkedAtomicQueue
import org.xyro.kumulus.KumulusTuple
import org.xyro.kumulus.collector.KumulusBoltCollector
import org.xyro.kumulus.collector.KumulusCollector
import org.xyro.kumulus.collector.KumulusSpoutCollector
import org.xyro.kumulus.grouping.AllGrouping
import org.xyro.kumulus.grouping.FieldsGrouping
import java.io.Serializable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

private val logger = KotlinLogging.logger {}

abstract class KumulusComponent(
        protected val config: Map<String, Any>,
        val context: TopologyContext
) {
    val inUse = AtomicBoolean(false)
    val isReady = AtomicBoolean(false)

    var onBusyBoltHook: ((String, Int, Long, Tuple) -> Unit)? = null

    protected val queue = MpscLinkedAtomicQueue<KumulusMessage>()

    /**
     * stream -> (component -> grouping)
     */
    lateinit var groupingStateMap: Map<String, Map<String, CustomStreamGrouping>>

    val waitStart = AtomicLong(0)
    val prepareStart = AtomicLong(0)

    val componentId = context.thisComponentId!!
    val taskId = context.thisTaskId

    private lateinit var thread: Thread

    fun prepare() {
        val groupingStateMap: MutableMap<String, MutableMap<String, CustomStreamGrouping>> = mutableMapOf()
        context.thisTargets.forEach { stream, groupings ->
            groupings.forEach { component, grouping ->
                val kGrouping = if (grouping.is_set_all) {
                    AllGrouping()
                } else if (grouping.is_set_none || grouping.is_set_shuffle || grouping.is_set_local_or_shuffle) {
                    ShuffleGrouping()
                } else if (grouping.is_set_fields) {
                    FieldsGrouping(grouping._fields, context.thisOutputFieldsForStreams[stream]!!)
                } else if  (grouping.is_set_custom_serialized) {
                    val customGrouping = Utils.javaDeserialize(grouping._custom_serialized, Serializable::class.java)!!
                    customGrouping as CustomStreamGrouping
                } else {
                    throw UnsupportedOperationException("Grouping type $grouping isn't currently supported by Kumulus")
                }
                kGrouping.prepare(this.context, GlobalStreamId(component, stream), context.getComponentTasks(component))
                groupingStateMap[stream] = (groupingStateMap[stream] ?: mutableMapOf()).also {
                    it[component] = kGrouping
                }
            }
        }
        this.groupingStateMap = groupingStateMap
        this.thread = Thread {
            queueProcessing()
        }.apply {
            isDaemon = true
            start()
        }
        isReady.set(true)
    }

    private fun queueProcessing() {
        while (true) {
            this.queue.drain { message ->
                try {
                    when (message) {
                        is PrepareMessage<*> -> {
                            this.prepareStart.set(System.nanoTime())
                            when (this) {
                                is KumulusSpout -> this.prepare(message.collector as KumulusSpoutCollector)
                                is KumulusBolt -> this.prepare(message.collector as KumulusBoltCollector)
                                else -> throw UnsupportedOperationException(
                                        "Class ${this.javaClass.canonicalName} is not a valid Kumulus component")
                            }
                        }
                        is ExecuteMessage -> {
                            if (this !is KumulusBolt) {
                                throw RuntimeException("Execute message got to a spout '${this.componentId}', this shouldn't happen.")
                            }
                            callBusyHook(this, message)
                            this.execute(message.tuple)
                        }
                    }
                }
            }
        }
    }

    private fun callBusyHook(bolt: KumulusBolt, message: ExecuteMessage) {
//        onBusyBoltHook?.let { onBusyBoltHook ->
//            val waitNanos = bolt.waitStart.getAndSet(0)
//            if (waitNanos > 0) {
//                try {
//                    scheduledExecutor.submit {
//                        try {
//                            onBusyBoltHook(
//                                    bolt.componentId,
//                                    bolt.taskId,
//                                    System.nanoTime() - waitNanos,
//                                    message.tuple.kTuple
//                            )
//                        } catch (e: Exception) {
//                            logger.error("An exception was thrown from busy-hook callback, ignoring", e)
//                        }
//                    }
//                } catch (e: Exception) {
//                    logger.error("An exception was thrown by busy-hook thread-pool submission, ignoring", e)
//                }
//            }
//        }
    }

    override fun toString(): String {
        return "[Component $componentId->$taskId]"
    }
}

abstract class KumulusMessage(val component: KumulusComponent)

abstract class PrepareMessage<in T: KumulusComponent>(
        component: KumulusComponent,
        val collector: KumulusCollector<in T>
) : KumulusMessage(component)

class SpoutPrepareMessage(component: KumulusComponent, collector: KumulusSpoutCollector) :
        PrepareMessage<KumulusSpout>(component, collector)

class BoltPrepareMessage(component: KumulusComponent, collector: KumulusBoltCollector) :
        PrepareMessage<KumulusBolt>(component, collector)

class ExecuteMessage(component: KumulusComponent, val tuple: KumulusTuple) :
        KumulusMessage(component)

class AckMessage(
        spout: KumulusSpout,
        val spoutMessageId: Any?,
        val ack: Boolean,
        val timeoutComponents: List<String>,
        val failedComponents: List<String>
) : KumulusMessage(spout)
