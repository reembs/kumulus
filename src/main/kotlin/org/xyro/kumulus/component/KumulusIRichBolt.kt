package org.xyro.kumulus.component

import org.apache.storm.task.OutputCollector
import org.apache.storm.topology.IRichBolt
import org.xyro.kumulus.KumulusTopologyContext

interface KumulusIRichBolt : IRichBolt {
    fun prepare(stormConf: Map<*, *>, context: KumulusTopologyContext, collector: OutputCollector)
}