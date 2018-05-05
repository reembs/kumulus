package org.oryx.kumulus

import org.apache.storm.Config
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.OutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IRichBolt
import org.apache.storm.topology.IRichSpout
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.apache.storm.utils.Utils
import org.junit.Test
import org.mockito.Matchers.eq
import org.mockito.Mockito.verify
import org.xyro.kumulus.KumulusStormTransformer
import org.xyro.kumulus.KumulusTopologyBuilder
import java.io.Serializable
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit

class KumulusTopologyBehaviorTest {
    @Test
    fun testMessageTimeout() {
        val builder = KumulusTopologyBuilder()
        val spout =  TestSpout(Output("field1"))
        builder.setSpout("spout", spout)
        builder.setBolt("bolt", TestBolt(Output()) { t, c ->

        })

        val config: MutableMap<String, Any> = mutableMapOf()
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 1L
        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 1L

        val topology = KumulusStormTransformer.initializeTopology(
                builder.createTopology(),
                config,
                "testtopology"
        )

        topology.prepare(10, TimeUnit.SECONDS)
        topology.start(false)

        spout.queue("item")

        Thread.sleep(2000)

        verify(spout).fail(eq(1))
    }
}

class TestBolt(
        private val output: Output,
        private val exec: (input: Tuple, collector: OutputCollector) -> Unit
) : IRichBolt {
    private lateinit var collector: OutputCollector

    override fun prepare(stormConf: MutableMap<Any?, Any?>, context: TopologyContext, collector: OutputCollector) {
        this.collector = collector
    }

    override fun cleanup() {}

    override fun getComponentConfiguration(): Map<String, Any> = mapOf()

    override fun execute(input: Tuple) {
        exec(input, collector)
    }

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        output.declare(declarer)
    }
}

open class SpoutTuple {
    val tuple: List<Any>

    constructor(vararg tuple: Any) : this(listOf(*tuple))

    constructor(tuple: List<Any>) {
        this.tuple = tuple
    }

    var stream = Utils.DEFAULT_STREAM_ID
    var messageId: Any? = null
}

open class TestSpout(private val output: Output): IRichSpout {
    private val queue: ArrayBlockingQueue<SpoutTuple> = ArrayBlockingQueue(1)
    private var currentMsgId = 0

    private lateinit var collector: SpoutOutputCollector

    fun queue(vararg tuple: Any) = this.queue(listOf(*tuple))

    fun queue(tuple: List<Any>) =
            queue(SpoutTuple(tuple).apply { messageId = nextId() })

    fun queue(tuple: SpoutTuple) = queue.add(tuple)

    private fun nextId(): Int = currentMsgId++

    override fun nextTuple() {
        queue.take().apply {
            collector.emit(stream, tuple, messageId)
        }
    }

    override fun deactivate() {}

    override fun activate() {}

    override fun open(conf: MutableMap<Any?, Any?>, context: TopologyContext, collector: SpoutOutputCollector) {
        this.collector = collector
    }

    override fun fail(msgId: Any?) {}

    override fun getComponentConfiguration(): Map<String, Any> = mapOf()

    override fun ack(msgId: Any?) {}

    override fun close() {}

    override fun declareOutputFields(declarer: OutputFieldsDeclarer) {
        output.declare(declarer)
    }
}

open class Output : Serializable {
    private val outputs = mutableMapOf<String, List<String>>()

    constructor(vararg fields: String) : this(Utils.DEFAULT_STREAM_ID, listOf(*fields))

    constructor(defaultStreamFields: List<String>) : this(Utils.DEFAULT_STREAM_ID, defaultStreamFields)

    constructor(stream: String, fields: List<String>) {
        outputs[stream] = fields
    }

    fun declare(declarer: OutputFieldsDeclarer) {
        for (output in outputs) {
            declarer.declareStream(output.key, Fields(output.value))
        }
    }

    fun put(stream: String, fields: List<String>) = outputs::put
}
