package org.oryx.kumulus.component

import org.apache.storm.task.TopologyContext
import org.oryx.kumulus.KumulusTuple
import org.oryx.kumulus.collector.KumulusCollector
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicBoolean

abstract class KumulusComponent(
        protected val config: MutableMap<String, Any>,
        protected val context: TopologyContext
) {
    public val inUse = AtomicBoolean(false)
    public val queue : Deque<KumulusMessage> = ConcurrentLinkedDeque()

    fun name(): String {
        return context.thisComponentId
    }

    fun taskId(): Int {
        return context.thisTaskId
    }
}

abstract class KumulusMessage(val type: Type) {
    enum class Type {
        PREPARE, EXECUTE
    }
}

class PrepareMessage(public val collector: KumulusCollector) : KumulusMessage(Type.PREPARE)

class ExecuteMessage(public val tuple: KumulusTuple) : KumulusMessage(Type.EXECUTE)
