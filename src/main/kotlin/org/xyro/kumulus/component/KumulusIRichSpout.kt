package org.xyro.kumulus.component

import org.apache.storm.spout.SpoutOutputCollector
import org.xyro.kumulus.KumulusTopologyContext

interface KumulusIRichSpout {
    fun open(conf: Map<*, *>, context: KumulusTopologyContext, collector: SpoutOutputCollector)
}