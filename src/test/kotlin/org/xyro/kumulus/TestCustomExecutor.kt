package org.xyro.kumulus

import org.apache.storm.Config
import org.apache.storm.topology.BasicOutputCollector
import org.apache.storm.tuple.Fields
import org.apache.storm.tuple.Tuple
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.xyro.kumulus.topology.KumulusTopologyBuilder
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.LinkedTransferQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class TestCustomExecutor {
    companion object {
        private const val TUPLE_COUNT = 100
        private const val THREAD_PREFIX = "custom-executor-"

        val executedOn: MutableSet<String> = ConcurrentHashMap.newKeySet()
        lateinit var acked: CountDownLatch
    }

    @Test(timeout = 10000)
    fun testComponentsRunOnProvidedExecutor() {
        val threadCounter = AtomicInteger(0)
        val executor =
            Executors.newFixedThreadPool(3) { r ->
                Thread(r, "$THREAD_PREFIX${threadCounter.getAndIncrement()}").apply { isDaemon = true }
            } as ThreadPoolExecutor

        try {
            runTopology(executor)
        } finally {
            executor.shutdownNow()
        }

        assertTrue("No work was recorded", executedOn.isNotEmpty())
        assertTrue(
            "Work ran outside the provided executor: $executedOn",
            executedOn.all { it.startsWith(THREAD_PREFIX) },
        )
        assertTrue("Executor did not run any tasks", executor.completedTaskCount > 0)
    }

    @Test(timeout = 10000)
    fun testDefaultExecutorUsesKumulusThreads() {
        runTopology(null)

        assertTrue("No work was recorded", executedOn.isNotEmpty())
        assertTrue(
            "Work ran outside Kumulus threads: $executedOn",
            executedOn.all { it.startsWith("KumulusThread-") },
        )
    }

    @Test
    fun testDefaultExecutorShape() {
        val executor = ExecutionPool.defaultExecutor(3)
        try {
            assertEquals(3, executor.corePoolSize)
            assertEquals(3, executor.maximumPoolSize)
            assertEquals(3, executor.poolSize)
            assertTrue(executor.queue is LinkedTransferQueue<*>)
            val thread = executor.threadFactory.newThread {}
            assertTrue(thread.isDaemon)
            assertTrue(thread.name.startsWith("KumulusThread-"))
        } finally {
            executor.shutdownNow()
        }
    }

    private fun runTopology(executor: Executor?) {
        executedOn.clear()
        acked = CountDownLatch(TUPLE_COUNT)

        val builder = KumulusTopologyBuilder()
        val config: MutableMap<String, Any> = mutableMapOf()
        config[Config.TOPOLOGY_MAX_SPOUT_PENDING] = 10L
        config[Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS] = 10L
        config[KumulusTopology.CONF_THREAD_POOL_CORE_SIZE] = 3L

        builder.setSpout("spout", CountingSpout())
        builder.setBolt("bolt", RecordingBolt()).shuffleGrouping("spout")

        val topology =
            KumulusStormTransformer.initializeTopology(builder.createTopology(), config, "test", executor)
        try {
            topology.prepare(5, TimeUnit.SECONDS)
            topology.start(false)
            assertTrue("Not all tuples were acked", acked.await(5, TimeUnit.SECONDS))
        } finally {
            topology.stop()
        }
    }

    class CountingSpout : DummySpout({ it.declare(Fields("n")) }) {
        private var emitted = 0

        override fun nextTuple() {
            if (emitted < TUPLE_COUNT) {
                collector.emit(listOf(emitted), emitted)
                emitted++
            }
        }

        override fun ack(msgId: Any?) {
            acked.countDown()
        }
    }

    class RecordingBolt : DummyBolt() {
        override fun execute(
            input: Tuple,
            collector: BasicOutputCollector,
        ) {
            executedOn.add(Thread.currentThread().name)
        }
    }
}
