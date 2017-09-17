package org.oryx.kumulus.component

import org.apache.storm.task.TopologyContext
import org.oryx.kumulus.KumulusTuple
import org.oryx.kumulus.collector.KumulusBoltCollector
import org.oryx.kumulus.collector.KumulusCollector
import org.oryx.kumulus.collector.KumulusSpoutCollector
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean

abstract class KumulusComponent(
        protected val config: MutableMap<String, Any>,
        val context: TopologyContext
) {
    val inUse = AtomicBoolean(false)
    val isReady = AtomicBoolean(false)

    fun name(): String {
        return context.thisComponentId
    }

    fun taskId(): Int {
        return context.thisTaskId
    }

    fun prepare() {
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
