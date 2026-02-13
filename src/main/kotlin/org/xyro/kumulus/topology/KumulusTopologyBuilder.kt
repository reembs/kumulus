package org.xyro.kumulus.topology

import org.apache.storm.Config
import org.apache.storm.generated.ComponentCommon
import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.generated.NullStruct
import org.apache.storm.generated.StreamInfo
import org.apache.storm.grouping.CustomStreamGrouping
import org.apache.storm.hooks.IWorkerHook
import org.apache.storm.shade.org.json.simple.JSONValue
import org.apache.storm.state.State
import org.apache.storm.topology.BasicBoltExecutor
import org.apache.storm.topology.IBasicBolt
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.IRichSpout
import org.apache.storm.topology.IRichStateSpout
import org.apache.storm.topology.IStatefulBolt
import org.apache.storm.topology.IStatefulWindowedBolt
import org.apache.storm.topology.IWindowedBolt
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.StatefulBoltExecutor
import org.apache.storm.topology.StatefulWindowedBoltExecutor
import org.apache.storm.topology.WindowedBoltExecutor
import org.apache.storm.tuple.Fields
import org.apache.storm.utils.Utils
import java.io.Serializable
import java.util.LinkedHashMap

@Suppress("unused", "MemberVisibilityCanBePrivate")
class KumulusTopologyBuilder {
    private val spouts: LinkedHashMap<String, KumulusDeclaredSpout> = LinkedHashMap()
    private val bolts: LinkedHashMap<String, KumulusDeclaredBolt> = LinkedHashMap()
    private val stateSpouts: LinkedHashMap<String, IRichStateSpout> = LinkedHashMap()

    fun setSpout(
        id: String,
        spout: IRichSpout,
    ): KumulusSpoutDeclarer {
        validateUnusedId(id)
        return registerSpout(id, spout, null)
    }

    fun setSpout(
        id: String,
        spout: IRichSpout,
        parallelismHint: Number,
    ): KumulusSpoutDeclarer {
        validateUnusedId(id)
        return registerSpout(id, spout, parallelismHint)
    }

    private fun registerSpout(
        id: String,
        spout: IRichSpout,
        parallelismHint: Number?,
    ): KumulusSpoutDeclarer {
        val common = ComponentCommon()
        common.set_inputs(mutableMapOf())
        initCommon(common, spout, parallelismHint)
        common.set_streams(declareStreams(spout))

        spouts[id] = KumulusDeclaredSpout(id, spout, common)
        return KumulusSpoutDeclarer(spouts[id]!!)
    }

    fun setBolt(
        id: String,
        bolt: IRichBolt,
    ): KumulusBoltDeclarer = setBoltInternal(id, bolt, null)

    private fun setBoltInternal(
        id: String,
        bolt: IRichBolt,
        parallelismHint: Number?,
    ): KumulusBoltDeclarer {
        validateUnusedId(id)
        return registerBolt(id, bolt, parallelismHint)
    }

    fun setBolt(
        id: String,
        bolt: IWindowedBolt,
    ): KumulusBoltDeclarer = setBoltInternal(id, WindowedBoltExecutor(bolt), null)

    fun setBolt(
        id: String,
        bolt: IWindowedBolt,
        parallelismHint: Number,
    ): KumulusBoltDeclarer = setBolt(id, WindowedBoltExecutor(bolt), parallelismHint)

    fun <T : State?> setBolt(
        id: String,
        bolt: IStatefulBolt<T>,
    ): KumulusBoltDeclarer = setBoltInternal(id, StatefulBoltExecutor(bolt), null)

    fun <T : State?> setBolt(
        id: String,
        bolt: IStatefulBolt<T>,
        parallelismHint: Number,
    ): KumulusBoltDeclarer = setBolt(id, StatefulBoltExecutor(bolt), parallelismHint)

    fun <T : State?> setBolt(
        id: String,
        bolt: IStatefulWindowedBolt<T>,
    ): KumulusBoltDeclarer = setBoltInternal(id, StatefulWindowedBoltExecutor(bolt), null)

    fun <T : State?> setBolt(
        id: String,
        bolt: IStatefulWindowedBolt<T>,
        parallelismHint: Number,
    ): KumulusBoltDeclarer = setBolt(id, StatefulWindowedBoltExecutor(bolt), parallelismHint)

    fun setBolt(
        id: String,
        bolt: IBasicBolt,
    ): KumulusBoltDeclarer = setBoltInternal(id, BasicBoltExecutor(bolt), null)

    fun setBolt(
        id: String,
        bolt: IBasicBolt,
        parallelismHint: Number,
    ): KumulusBoltDeclarer = setBolt(id, BasicBoltExecutor(bolt), parallelismHint)

    fun setBolt(
        id: String,
        bolt: IRichBolt,
        parallelismHint: Number,
    ): KumulusBoltDeclarer {
        validateUnusedId(id)
        return registerBolt(id, bolt, parallelismHint)
    }

    private fun registerBolt(
        id: String,
        bolt: IRichBolt,
        parallelismHint: Number?,
    ): KumulusBoltDeclarer {
        val common = ComponentCommon()
        common.set_inputs(mutableMapOf())
        initCommon(common, bolt, parallelismHint)
        common.set_streams(declareStreams(bolt))

        bolts[id] = KumulusDeclaredBolt(id, bolt, common)
        return KumulusBoltDeclarer(bolts[id]!!)
    }

    fun createTopology(): KumulusTopologyDefinition = KumulusTopologyDefinition(spouts.toMap(), bolts.toMap())

    fun setStateSpout(
        id: String,
        spout: IRichStateSpout,
    ) {
        setStateSpout(id, spout, 1)
    }

    fun setStateSpout(
        id: String,
        spout: IRichStateSpout,
        parallelismHint: Number,
    ) {
        validateUnusedId(id)
        stateSpouts[id] = spout
        throw UnsupportedOperationException("State spouts are not supported in KumulusTopologyBuilder")
    }

    fun addWorkerHook(hook: IWorkerHook?) {
        if (hook == null) {
            throw IllegalArgumentException("WorkerHook must not be null.")
        }
        // Kumulus runs in-process and does not have worker-level hooks.
    }

    private fun initCommon(
        common: ComponentCommon,
        component: Any,
        parallelismHint: Number?,
    ) {
        if (parallelismHint != null) {
            val parallelism = parallelismHint.toInt()
            if (parallelism < 1) {
                throw IllegalArgumentException("Parallelism must be positive.")
            }
            common.set_parallelism_hint(parallelism)
        }

        val config =
            when (component) {
                is IRichBolt -> component.componentConfiguration
                is IRichSpout -> component.componentConfiguration
                else -> null
            }
        if (config != null) {
            common.set_json_conf(JSONValue.toJSONString(config))
        }
    }

    private fun validateUnusedId(id: String) {
        if (bolts.containsKey(id)) {
            throw IllegalArgumentException("Bolt has already been declared for id$id")
        }
        if (spouts.containsKey(id)) {
            throw IllegalArgumentException("Spout has already been declared for id$id")
        }
        if (stateSpouts.containsKey(id)) {
            throw IllegalArgumentException("State spout has already been declared for id$id")
        }
    }

    private fun declareStreams(component: Any): MutableMap<String, StreamInfo> {
        val declarer = KumulusOutputFieldsCollector()

        when (component) {
            is IRichBolt -> component.declareOutputFields(declarer)
            is IRichSpout -> component.declareOutputFields(declarer)
            else -> throw IllegalArgumentException("Component type ${component::class.qualifiedName} is unsupported")
        }

        return declarer.streams
    }
}

data class KumulusTopologyDefinition(
    val spouts: Map<String, KumulusDeclaredSpout>,
    val bolts: Map<String, KumulusDeclaredBolt>,
)

data class KumulusDeclaredSpout(
    val id: String,
    val spout: IRichSpout,
    val common: ComponentCommon,
)

data class KumulusDeclaredBolt(
    val id: String,
    val bolt: IRichBolt,
    val common: ComponentCommon,
)

class KumulusSpoutDeclarer internal constructor(
    private val registration: KumulusDeclaredSpout,
) {
    fun addConfiguration(
        configKey: String,
        configValue: Any?,
    ): KumulusSpoutDeclarer = addConfigurations(mapOf(configKey to configValue))

    fun addConfigurations(conf: Map<String, Any?>): KumulusSpoutDeclarer {
        if (conf.containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
            throw IllegalArgumentException("Cannot set serializations for a component using fluent API")
        }
        val merged = mergeWithExistingJsonConfig(registration.common, conf)
        registration.common.set_json_conf(JSONValue.toJSONString(merged))
        return this
    }

    fun setNumTasks(numTasks: Int): KumulusSpoutDeclarer {
        registration.common.set_parallelism_hint(numTasks.coerceAtLeast(1))
        return this
    }
}

class KumulusBoltDeclarer internal constructor(
    private val registration: KumulusDeclaredBolt,
) {
    fun fieldsGrouping(
        componentId: String,
        fields: Fields,
    ): KumulusBoltDeclarer = fieldsGrouping(componentId, Utils.DEFAULT_STREAM_ID, fields)

    fun fieldsGrouping(
        componentId: String,
        streamId: String,
        fields: Fields,
    ): KumulusBoltDeclarer =
        grouping(
            componentId,
            streamId,
            Grouping().apply { set_fields(fields.toList()) },
        )

    fun shuffleGrouping(componentId: String): KumulusBoltDeclarer = shuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID)

    fun shuffleGrouping(
        componentId: String,
        streamId: String,
    ): KumulusBoltDeclarer =
        grouping(
            componentId,
            streamId,
            Grouping().apply {
                set_shuffle(NullStruct())
            },
        )

    fun localOrShuffleGrouping(componentId: String): KumulusBoltDeclarer = localOrShuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID)

    fun localOrShuffleGrouping(
        componentId: String,
        streamId: String,
    ): KumulusBoltDeclarer =
        grouping(
            componentId,
            streamId,
            Grouping().apply {
                set_local_or_shuffle(NullStruct())
            },
        )

    fun noneGrouping(componentId: String): KumulusBoltDeclarer = noneGrouping(componentId, Utils.DEFAULT_STREAM_ID)

    fun noneGrouping(
        componentId: String,
        streamId: String,
    ): KumulusBoltDeclarer =
        grouping(
            componentId,
            streamId,
            Grouping().apply {
                set_none(NullStruct())
            },
        )

    fun allGrouping(componentId: String): KumulusBoltDeclarer = allGrouping(componentId, Utils.DEFAULT_STREAM_ID)

    fun allGrouping(
        componentId: String,
        streamId: String,
    ): KumulusBoltDeclarer =
        grouping(
            componentId,
            streamId,
            Grouping().apply {
                set_all(NullStruct())
            },
        )

    fun directGrouping(componentId: String): KumulusBoltDeclarer = directGrouping(componentId, Utils.DEFAULT_STREAM_ID)

    fun directGrouping(
        componentId: String,
        streamId: String,
    ): KumulusBoltDeclarer =
        grouping(
            componentId,
            streamId,
            Grouping().apply {
                set_direct(NullStruct())
            },
        )

    fun customGrouping(
        componentId: String,
        grouping: CustomStreamGrouping,
    ): KumulusBoltDeclarer = customGrouping(componentId, Utils.DEFAULT_STREAM_ID, grouping)

    fun customGrouping(
        componentId: String,
        streamId: String,
        grouping: CustomStreamGrouping,
    ): KumulusBoltDeclarer {
        if (grouping !is Serializable) {
            throw IllegalArgumentException("Custom grouping must be serializable for Kumulus")
        }
        return this.grouping(
            componentId,
            streamId,
            Grouping().apply { set_custom_serialized(Utils.javaSerialize(grouping)) },
        )
    }

    fun grouping(
        id: GlobalStreamId,
        grouping: Grouping,
    ): KumulusBoltDeclarer {
        registration.common.put_to_inputs(id, grouping)
        return this
    }

    fun grouping(
        componentId: String,
        streamId: String,
        grouping: Grouping,
    ): KumulusBoltDeclarer = grouping(GlobalStreamId(componentId, streamId), grouping)

    fun addConfiguration(
        configKey: String,
        configValue: Any?,
    ): KumulusBoltDeclarer = addConfigurations(mapOf(configKey to configValue))

    fun addConfigurations(conf: Map<String, Any?>): KumulusBoltDeclarer {
        if (conf.containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
            throw IllegalArgumentException("Cannot set serializations for a component using fluent API")
        }
        val merged = mergeWithExistingJsonConfig(registration.common, conf)
        registration.common.set_json_conf(JSONValue.toJSONString(merged))
        return this
    }

    fun setNumTasks(numTasks: Int): KumulusBoltDeclarer {
        registration.common.set_parallelism_hint(numTasks.coerceAtLeast(1))
        return this
    }
}

private class KumulusOutputFieldsCollector : OutputFieldsDeclarer {
    val streams: MutableMap<String, StreamInfo> = mutableMapOf()

    override fun declare(fields: Fields) {
        declare(false, fields)
    }

    override fun declare(
        direct: Boolean,
        fields: Fields,
    ) {
        declareStream(Utils.DEFAULT_STREAM_ID, direct, fields)
    }

    override fun declareStream(
        streamId: String,
        fields: Fields,
    ) {
        declareStream(streamId, false, fields)
    }

    override fun declareStream(
        streamId: String,
        direct: Boolean,
        fields: Fields,
    ) {
        streams[streamId] =
            StreamInfo().apply {
                set_output_fields(fields.toList())
                set_direct(direct)
            }
    }
}

private fun mergeWithExistingJsonConfig(
    common: ComponentCommon,
    update: Map<String, Any?>,
): MutableMap<String, Any?> {
    val existing = parseJsonObject(common._json_conf)
    val merged = HashMap(existing)
    merged.putAll(update)
    return merged
}

@Suppress("UNCHECKED_CAST")
private fun parseJsonObject(json: String?): MutableMap<String, Any?> {
    if (json == null) {
        return mutableMapOf()
    }
    return try {
        (JSONValue.parseWithException(json) as Map<String, Any?>).toMutableMap()
    } catch (e: Exception) {
        throw RuntimeException(e)
    }
}
