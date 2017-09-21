package org.oryx.kumulus.component

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.grouping.CustomStreamGrouping
import org.apache.storm.grouping.ShuffleGrouping
import org.apache.storm.task.TopologyContext
import org.apache.storm.utils.Utils
import org.oryx.kumulus.KumulusTuple
import org.oryx.kumulus.collector.KumulusBoltCollector
import org.oryx.kumulus.collector.KumulusCollector
import org.oryx.kumulus.collector.KumulusSpoutCollector
import org.oryx.kumulus.grouping.AllGrouping
import org.oryx.kumulus.grouping.FieldsGrouping
import java.io.Serializable
import java.util.concurrent.atomic.AtomicBoolean

abstract class KumulusComponent(
        protected val config: MutableMap<String, Any>,
        val context: TopologyContext
) {
    val inUse = AtomicBoolean(false)
    val isReady = AtomicBoolean(false)

    lateinit var groupingStateMap: Map<String, Map<String, CustomStreamGrouping>>

    fun name(): String {
        return context.thisComponentId
    }

    fun taskId(): Int {
        return context.thisTaskId
    }

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
        isReady.set(true)
    }

    override fun toString(): String {
        return "[Component ${context.thisComponentId}->${context.thisTaskId}]"
    }
}

fun KumulusComponent.isSpout() : Boolean {
    return when(this) {
        is KumulusSpout -> true
        is KumulusBolt -> false
        else -> null
    } ?: throw UnsupportedOperationException()
}

abstract class KumulusMessage(val type: Type, val component: KumulusComponent) {
    enum class Type {
        PREPARE, EXECUTE, ACK
    }
}

abstract class PrepareMessage<in T: KumulusComponent>(
        component: KumulusComponent,
        val collector: KumulusCollector<in T>
) :
        KumulusMessage(Type.PREPARE, component)

class SpoutPrepareMessage(component: KumulusComponent, collector: KumulusSpoutCollector) :
        PrepareMessage<KumulusSpout>(component, collector)

class BoltPrepareMessage(component: KumulusComponent, collector: KumulusBoltCollector) :
        PrepareMessage<KumulusBolt>(component, collector)

class ExecuteMessage(component: KumulusComponent, val tuple: KumulusTuple) :
        KumulusMessage(Type.EXECUTE, component)

class AckMessage(spout: KumulusSpout, val spoutMessageId: Any?, val ack: Boolean) : KumulusMessage(Type.ACK, spout)
