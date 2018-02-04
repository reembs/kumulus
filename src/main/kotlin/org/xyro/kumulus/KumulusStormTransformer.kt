package org.xyro.kumulus

import clojure.lang.Atom
import org.apache.storm.generated.*
import org.apache.storm.metric.api.IMetric
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.*
import org.apache.storm.tuple.Fields
import org.apache.storm.utils.Utils
import java.io.Serializable

@Suppress("unused")
class KumulusStormTransformer {
    companion object {
        /**
         * Initialize a Kumulus topology from a {@link StormTopology}
         * @param topology the Storm topology to transform
         * @param rawConfig the Storm configuration
         * @param stormId the Storm topology ID string
         */
        @Suppress("UNCHECKED_CAST")
        @JvmStatic
        fun initializeTopology(topology: StormTopology, rawConfig: MutableMap<String, Any>, stormId: String) : KumulusTopology {
            val builder = KumulusTopologyBuilder()

            val getter: (String) -> ComponentCommon = { id ->
                if (topology._bolts?.containsKey(id)!!) {
                    topology._bolts[id]!!._common
                } else {
                    topology._spouts[id]!!._common
                }
            }

            val boltField = StormTopology::class.java.getDeclaredField("bolts")!!
            boltField.isAccessible = true
            val serializedBoltsMap : Map<String, Bolt> = boltField.get(topology) as Map<String, Bolt>
            serializedBoltsMap.entries.forEach { (id, bolt) ->
                val boltObject = bolt._bolt_object!!
                val componentCommon = getter(id)
                builder.setBolt(id, StormComponentData(
                        topology,
                        boltObject._serialized_java,
                        componentCommon._parallelism_hint
                ))
            }

            val spoutField = StormTopology::class.java.getDeclaredField("spouts")!!
            spoutField.isAccessible = true
            val serializedSpoutsMap : Map<String, SpoutSpec> = spoutField.get(topology) as Map<String, SpoutSpec>
            serializedSpoutsMap.entries.forEach { (id, spout) ->
                val spoutObject = spout._spout_object!!
                val componentCommon = getter(id)
                builder.setSpout(id, StormComponentData(
                        topology,
                        spoutObject._serialized_java,
                        componentCommon._parallelism_hint
                ))
            }

            //val taskToComponent = mutableMapOf<Int, String>()

//            val componentToSortedTasks = mutableMapOf<String, List<Int>>()
//            val componentToStreamToFields = mutableMapOf<String, Map<String, Fields>>()
//
//            rawConfig[Config.TOPOLOGY_NAME] = stormId
//
//            componentToSortedTasks[Constants.SYSTEM_COMPONENT_ID] = listOf(Constants.SYSTEM_TASK_ID.toInt())
//            componentToStreamToFields[Constants.SYSTEM_COMPONENT_ID] =
//                    mapOf(Constants.SYSTEM_TICK_STREAM_ID to Fields())
//
//            val kComponents : MutableList<KumulusComponent> = mutableListOf()
//
//            val kComponentInputs : MutableMap<Pair<String, GlobalStreamId>, org.apache.storm.generated.Grouping> =
//                    mutableMapOf()

//            var id = 1
            for ((name, component) in builder.boltsMap + builder.spoutsMap) {
//                for (i in 1..component.getParallelism()) {
//                    taskToComponent[id] = name
//
//                    val tasks: MutableList<Int> = if (!componentToSortedTasks.containsKey(name)) {
//                        componentToSortedTasks[name] = mutableListOf()
//                        componentToSortedTasks[name] as MutableList<Int>
//                    } else {
//                        componentToSortedTasks[name] as MutableList<Int>
//                    }
//
//                    tasks.add(id)
//
//                    val streamToFields: MutableMap<String, Fields> = if (componentToStreamToFields.containsKey(name)) {
//                        componentToStreamToFields[name] as MutableMap<String, Fields>
//                    } else {
//                        componentToStreamToFields[name] = mutableMapOf()
//                        componentToStreamToFields[name] as MutableMap<String, Fields>
//                    }

                    val componentCommon = getter(name)
                    for (stream in componentCommon._streams.keys) {
                        val streamInfo = componentCommon._streams[stream]
//                        streamToFields[stream] = Fields(streamInfo?._output_fields)
                        builder.declareOutputFields(name, stream, streamInfo!!)
                    }

                    componentCommon._inputs?.forEach({
//                        kComponentInputs[name to it.key] = it.value
                        builder.declareComponentInput(name, it.key, it.value)
                    })

//                    id++
//                }
            }

            val config = rawConfig.mapValues { it ->
                return@mapValues (it.value as? Int)?.toLong() ?: it.value
            }

//            componentToSortedTasks.forEach({ componentId: String, taskIds: List<Int> ->
//                val (componentObjectSerialized, componentCommon) =
//                        when {
//                            topology._spouts!!.containsKey(componentId) ->
//                                topology._spouts[componentId]!!
//                                        .let { it._spout_object!! to it._common!! }
//                            topology._bolts!!.containsKey(componentId) ->
//                                topology._bolts[componentId]!!
//                                        .let { it._bolt_object!! to it._common!! }
//                            componentId == Constants.SYSTEM_COMPONENT_ID ->
//                                    null to null
//                            else -> throw Exception("Component name '$componentId' was not found in underlaying topology object")
//                        }
//
//                taskIds.forEach({ taskId ->
//                    val componentInstance =
//                            if (taskId == Constants.SYSTEM_TASK_ID.toInt()) {
//                                BasicBoltExecutor(object : BaseBasicBolt() {
//                                    override fun execute(input: Tuple?, collector: BasicOutputCollector?) {}
//                                    override fun declareOutputFields(declarer: OutputFieldsDeclarer?) {
//                                        // Declared hard-coded
//                                    }
//                                })
//                            } else {
//                                Utils.javaDeserialize<Serializable>(componentObjectSerialized!!._serialized_java, Serializable::class.java)
//                            }
//
//                    kComponents += when (componentInstance) {
//                        is IRichBolt -> {
//                            KumulusBolt(
//                                    config,
//                                    context,
//                                    componentInstance,
//                                    componentCommon?._inputs?.toMap() ?: mapOf(),
//                                    componentCommon?._streams?.toMap() ?: mapOf()
//                            ).apply {
//                                (componentInstance.componentConfiguration ?: mapOf())[Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS]?.let { secs ->
//                                    assert(secs is Number)
//                                    (secs as Number).let {
//                                        this.tickSecs = secs
//                                    }
//                                }
//                            }
//                        }
//                        is IRichSpout -> KumulusSpout(config, context, componentInstance)
//                        else ->
//                            throw Throwable("Component of type ${componentInstance::class.qualifiedName} is not acceptable by Kumulus")
//                    }
//                })
//            })

            val kumulusTopology = builder.build(config)
            kumulusTopology.validateTopology()
            return kumulusTopology
        }
    }

    class KumulusTopologyValidationException(msg: String) : Exception(msg)

    class StormComponentData(
            private val topology: StormTopology,
            private val serializedComponent: ByteArray,
            private val parallelism: Int
    ) : ComponentData {

        override fun createInstance(): IComponent {
            return Utils.javaDeserialize<Serializable>(serializedComponent, Serializable::class.java) as IComponent
        }

        override fun getParallelism(): Int {
            return parallelism
        }

        override fun createContext(
                config: Map<String, Any>,
                taskToComponent: Map<Int, String>,
                componentToSortedTasks: Map<String, List<Int>>,
                componentToStreamToFields: Map<String, Map<String, Fields>>,
                stormId: String,
                taskId: Int
        ): Any {
            val codeDir = "/tmp"
            val pidDir = "/tmp"
            val workerPort = 6699
            val workerTasks : List<Int> = listOf()
            val defaultResources : Map<String, Any> = LinkedHashMap()
            val userResources : Map<String, Any> = LinkedHashMap()
            val executorData : Map<String, Any> = LinkedHashMap()
            val registeredMetrics : Map<Int, Map<Int, Map<String, IMetric>>> = LinkedHashMap()

            return TopologyContext(
                    topology,
                    config,
                    taskToComponent,
                    componentToSortedTasks,
                    componentToStreamToFields,
                    stormId,
                    codeDir,
                    pidDir,
                    taskId,
                    workerPort,
                    workerTasks,
                    defaultResources,
                    userResources,
                    executorData,
                    registeredMetrics,
                    Atom(Object()))
        }

    }
}