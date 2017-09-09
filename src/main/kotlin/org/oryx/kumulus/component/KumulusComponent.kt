package org.oryx.kumulus.component

import org.apache.storm.task.TopologyContext
import java.util.concurrent.atomic.AtomicBoolean

abstract class KumulusComponent(
        protected val config: MutableMap<String, Any>,
        protected val context: TopologyContext
) {
    public val inUse = AtomicBoolean(false)

    fun name(): String {
        return context.thisComponentId
    }

    fun taskId(): Int {
        return context.thisTaskId
    }

    fun taskIndex(): Int {
        return context.thisTaskIndex
    }
}