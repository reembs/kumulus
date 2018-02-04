package org.xyro.kumulus

import org.apache.storm.Constants
import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.generated.StreamInfo
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.*
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.xyro.kumulus.component.KumulusBolt
import org.xyro.kumulus.component.KumulusComponent
import org.xyro.kumulus.component.KumulusIRichBolt
import org.xyro.kumulus.component.KumulusSpout

class KumulusTopologyBuilder {
    private val spouts: MutableMap<String, ComponentData> = mutableMapOf()
    private val bolts: MutableMap<String, ComponentData> = mutableMapOf()
    private val streams: MutableMap<String, MutableMap<String, StreamInfo>> = mutableMapOf()
    private val inputs: MutableMap<String, MutableMap<GlobalStreamId, Grouping>> = mutableMapOf()
    private val systemComponent = object: ComponentData {
        override fun createInstance(): IComponent {
            return object : KumulusIRichBolt {
                override fun prepare(stormConf: Map<*, *>, context: KumulusTopologyContext, collector: OutputCollector) {}

                override fun prepare(stormConf: MutableMap<Any?, Any?>?, context: TopologyContext?, collector: OutputCollector?) {}

                override fun cleanup() {}

                override fun getComponentConfiguration(): MutableMap<String, Any> = mutableMapOf()

                override fun execute(input: Tuple?) {}

                override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
                    // Declared hard-coded
                }
            }
        }

        override fun getParallelism(): Int = 1

        override fun createContext(
                config: Map<String, Any>,
                taskToComponent: Map<Int, String>,
                componentToSortedTasks: Map<String, List<Int>>,
                componentToStreamToFields: Map<String, Map<String, Fields>>,
                stormId: String,
                taskId: Int
        ): Any {
            return KumulusTopologyContext()
        }
    }

    val boltsMap: Map<String, ComponentData>
        get() {
            return toComponentsMap(bolts)
        }

    val spoutsMap: Map<String, ComponentData>
        get() {
            return toComponentsMap(spouts)
        }

    private fun toComponentsMap(componentsData: Map<String, ComponentData>) : Map<String, ComponentData> {
        return componentsData.toMap()
    }

    private fun getInputs(componentName: String): Map<GlobalStreamId, Grouping> {
        return inputs[componentName]?.toMap() ?: mapOf()
    }

    private fun getOutputs(componentName: String): Map<String, StreamInfo> {
        return streams[componentName]?.toMap() ?: mapOf()
    }

    fun setBolt(id: String, componentData: ComponentData) {
        this.bolts[id] = componentData
    }

    fun setSpout(id: String, componentData: KumulusStormTransformer.StormComponentData) {
        this.spouts[id] = componentData
    }

    fun declareOutputFields(name: String, stream: String, info: StreamInfo) {
        this.streams[name] = this.streams[name] ?: mutableMapOf()
        this.streams[name]!![stream] = info
    }

    fun declareComponentInput(name: String, globalStreamId: GlobalStreamId, grouping: Grouping) {
        this.inputs[name] = this.inputs[name] ?: mutableMapOf()
        this.inputs[name]!![globalStreamId] = grouping
    }

    fun build(rawConfig: Map<String, Any>): KumulusTopology {
        val boltsMap : Map<String, ComponentData> = this.boltsMap
        val spoutsMap : Map<String, ComponentData> = this.spoutsMap

        val taskToComponent = mutableMapOf(
                Constants.SYSTEM_TASK_ID.toInt() to Constants.SYSTEM_COMPONENT_ID
        )

        val componentMaps = listOf(spoutsMap, boltsMap)

        val kComponents : MutableList<KumulusComponent> = mutableListOf()

        for (componentMap in componentMaps) {
            for ((name, component) in componentMap) {
                val parallelism = Math.max(component.getParallelism(), 1)
                for (i in 1..parallelism) {
                    taskToComponent[taskToComponent.size] = name // this sets the taskId
                }
            }
        }

        val config = rawConfig.mapValues { it ->
            return@mapValues (it.value as? Int)?.toLong() ?: it.value
        }

        val componentDataMap = mutableMapOf<Int, ComponentData>()

        taskToComponent.forEach { taskId, componentId ->
            val component =
                    when {
                        componentId == Constants.SYSTEM_COMPONENT_ID -> systemComponent

                        this.spoutsMap.containsKey(componentId) ->
                            this.spoutsMap[componentId]!!

                        this.boltsMap.containsKey(componentId) ->
                            this.boltsMap[componentId]!!

                        else -> throw Exception("Component name '$componentId' was not found in underlying topology object")
                    }


            val componentInstance = component.createInstance()

            componentDataMap[taskId] = component

            kComponents += when (componentInstance) {
                is IRichBolt -> {
                    KumulusBolt(
                            config,
                            componentId,
                            taskId,
                            componentInstance,
                            this.getInputs(componentId),
                            this.getOutputs(componentId)
                    )
                }
                is IRichSpout -> KumulusSpout(
                        config,
                        componentId,
                        taskId,
                        componentInstance
                )
                else ->
                    throw Throwable("Component of type ${componentInstance::class.qualifiedName} is not acceptable by Kumulus")
            }
        }

        val kumulusTopology = KumulusTopology(
                kComponents,
                config,
                componentDataMap,
                streams
        )
        kumulusTopology.validateTopology()
        return kumulusTopology
    }
}