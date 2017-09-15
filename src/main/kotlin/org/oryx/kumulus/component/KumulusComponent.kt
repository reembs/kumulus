package org.oryx.kumulus.component

import com.sun.org.apache.xpath.internal.operations.Bool
import org.apache.storm.task.TopologyContext
import org.oryx.kumulus.KumulusTuple
import org.oryx.kumulus.collector.KumulusBoltCollector
import org.oryx.kumulus.collector.KumulusCollector
import org.oryx.kumulus.collector.KumulusSpoutCollector
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicBoolean

abstract class KumulusComponent(
        protected val config: MutableMap<String, Any>,
        val context: TopologyContext
) {
    public val inUse = AtomicBoolean(false)
    public val isReady = AtomicBoolean(false)
    public val queue : Deque<KumulusMessage> = ConcurrentLinkedDeque()

    fun name(): String {
        return context.thisComponentId
    }

    fun taskId(): Int {
        return context.thisTaskId
    }

    fun prepare() {
        isReady.set(true)
    }
}

fun KumulusComponent.isSpout() : Boolean {
    return when(this) {
        is KumulusSpout -> true
        is KumulusBolt -> false
        else -> null
    } ?: throw UnsupportedOperationException()
}

abstract class KumulusMessage(val type: Type) {
    enum class Type {
        PREPARE, EXECUTE
    }
}

abstract class PrepareMessage<in T: KumulusComponent>(val collector: KumulusCollector<in T>) :
        KumulusMessage(Type.PREPARE)

class SpoutPrepareMessage(collector: KumulusSpoutCollector) :
        PrepareMessage<KumulusSpout>(collector)

class BoltPrepareMessage(collector: KumulusBoltCollector) :
        PrepareMessage<KumulusBolt>(collector)

class ExecuteMessage(val tuple: KumulusTuple) : KumulusMessage(Type.EXECUTE)
