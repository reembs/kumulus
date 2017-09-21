package org.oryx.kumulus

import org.oryx.kumulus.component.KumulusComponent
import org.oryx.kumulus.component.KumulusSpout

interface KumulusEmitter {
    fun getDestinations(tasks: List<Int>): List<KumulusComponent>
    fun execute(destComponent: KumulusComponent, kumulusTuple: KumulusTuple)
    fun completeMessageProcessing(spout: KumulusSpout, spoutMessageId: Any?, ack: Boolean)
}