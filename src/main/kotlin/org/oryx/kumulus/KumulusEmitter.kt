package org.oryx.kumulus

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.component.KumulusComponent
import org.oryx.kumulus.component.KumulusSpout

interface KumulusEmitter {
    fun getDestinations(
            self: KumulusComponent,
            dest: GlobalStreamId,
            grouping: Grouping,
            tuple: List<Any>,
            anchors: Collection<Tuple>?
    ): List<KumulusComponent>

    fun execute(destComponent: KumulusComponent, kumulusTuple: KumulusTuple)
    fun completeMessageProcessing(spout: KumulusSpout, spoutMessageId: Any?, ack: Boolean)
}