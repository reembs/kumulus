package org.oryx.kumulus

import org.apache.storm.Config
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.IComponent

abstract class KumulusComponent(
        protected val config: Config,
        protected val context: TopologyContext,
        protected val componentInstance: IComponent
) {
    abstract fun prepare()
}