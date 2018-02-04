package org.xyro.kumulus.component

import org.apache.storm.Constants
import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.grouping.CustomStreamGrouping
import org.apache.storm.grouping.ShuffleGrouping
import org.apache.storm.task.TopologyContext
import org.apache.storm.tuple.Fields
import org.apache.storm.utils.Utils
import org.xyro.kumulus.KumulusTopologyContext
import org.xyro.kumulus.KumulusTuple
import org.xyro.kumulus.collector.KumulusBoltCollector
import org.xyro.kumulus.collector.KumulusCollector
import org.xyro.kumulus.collector.KumulusSpoutCollector
import org.xyro.kumulus.grouping.AllGrouping
import org.xyro.kumulus.grouping.FieldsGrouping
import java.io.Serializable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

@Suppress("MemberVisibilityCanPrivate")
abstract class KumulusComponent(
        protected val config: Map<String, Any>,
        val componentId: String,
        val taskId: Int
) {
    val inUse: AtomicBoolean = AtomicBoolean(false)
    val isReady: AtomicBoolean = AtomicBoolean(false)
    val waitStart: AtomicLong = AtomicLong(0)
    val prepareStart: AtomicLong = AtomicLong(0)

    protected var context: TopologyContext? = null
    protected var kContext: KumulusTopologyContext? = null

    /**
     * stream -> (component -> grouping)
     */
    lateinit var groupingStateMap: Map<String, Map<String, CustomStreamGrouping>>

    private val emptyFields = Fields()

    fun prepare() {
        if (this.taskId != Constants.SYSTEM_TASK_ID.toInt()) {
            assert(this.context != null) { "setContext wasn't called before prepare()" }
            val groupingStateMap: MutableMap<String, MutableMap<String, CustomStreamGrouping>> = mutableMapOf()
            getThisTargets().forEach { stream, groupings ->
                groupings.forEach { component, grouping ->
                    val kGrouping = if (grouping.is_set_all) {
                        AllGrouping()
                    } else if (grouping.is_set_none || grouping.is_set_shuffle || grouping.is_set_local_or_shuffle) {
                        ShuffleGrouping()
                    } else if (grouping.is_set_fields) {
                        FieldsGrouping(grouping._fields, getThisOutputFields(stream))
                    } else if (grouping.is_set_custom_serialized) {
                        val customGrouping = Utils.javaDeserialize(grouping._custom_serialized, Serializable::class.java)!!
                        customGrouping as CustomStreamGrouping
                    } else {
                        throw UnsupportedOperationException("Grouping type $grouping isn't currently supported by Kumulus")
                    }
                    kGrouping.prepare(this.context, GlobalStreamId(component, stream), getComponentTasks(component))
                    groupingStateMap[stream] = (groupingStateMap[stream] ?: mutableMapOf()).also {
                        it[component] = kGrouping
                    }
                }
            }
            this.groupingStateMap = groupingStateMap
        }
        isReady.set(true)
    }

    fun setContext(context: Any) {
        this.context = context as? TopologyContext
        this.kContext = context as? KumulusTopologyContext
    }

    override fun toString(): String {
        return "[Component $componentId->$taskId]"
    }

    protected fun getThisTaskIndex(): Int {
        return context?.thisTaskIndex ?: kContext?.taskIndex ?: throw IllegalArgumentException()
    }

    private fun getThisTargets(): Map<String, MutableMap<String, Grouping>> {
        return context?.thisTargets ?: kContext?.targets ?: throw IllegalArgumentException()
    }

    private fun getComponentTasks(componentId: String): List<Int> {
        return context?.getComponentTasks(componentId) ?: kContext?.componentTasks ?: throw IllegalArgumentException()
    }

    private fun getThisOutputFields(stream: String): List<String> {
        return context?.thisOutputFieldsForStreams!![stream] ?:
                kContext?.outputFields!![stream] ?:
                throw IllegalArgumentException("stream: $stream")
    }

    fun getOutputFields(stream: String): Fields {
        if (this.taskId == Constants.SYSTEM_TASK_ID.toInt() && stream == Constants.SYSTEM_TICK_STREAM_ID) {
            return emptyFields
        }
        return context?.getThisOutputFields(stream) ?:
                kContext?.outputFields?.get(stream)?.let { Fields(it) } ?:
                throw IllegalArgumentException("stream: $stream")
    }

}

fun KumulusComponent.isSpout() : Boolean {
    return when(this) {
        is KumulusSpout -> true
        is KumulusBolt -> false
        else -> null
    } ?: throw UnsupportedOperationException()
}

abstract class KumulusMessage(val component: KumulusComponent)

abstract class PrepareMessage<in T: KumulusComponent>(
        component: KumulusComponent,
        val context: Any,
        val collector: KumulusCollector<in T>
) : KumulusMessage(component)

class SpoutPrepareMessage(component: KumulusComponent, context: Any, collector: KumulusSpoutCollector) :
        PrepareMessage<KumulusSpout>(component, context, collector)

class BoltPrepareMessage(component: KumulusComponent, context: Any, collector: KumulusBoltCollector) :
        PrepareMessage<KumulusBolt>(component, context, collector)

class ExecuteMessage(component: KumulusComponent, val tuple: KumulusTuple) :
        KumulusMessage(component)

class AckMessage(spout: KumulusSpout, val spoutMessageId: Any?, val ack: Boolean) : KumulusMessage(spout)
