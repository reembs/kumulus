package org.xyro.kumulus

import org.xyro.kumulus.component.KumulusComponent
import org.xyro.kumulus.component.KumulusSpout

interface KumulusEmitter {
    fun getDestinations(tasks: List<Int>): List<KumulusComponent>
    fun execute(destComponent: KumulusComponent, kumulusTuple: KumulusTuple)
    fun completeMessageProcessing(
        spout: KumulusSpout,
        spoutMessageId: Any?,
        timeoutTasks: List<Int>,
        failedTasks: List<Int>
    )
    fun throwException(t: Throwable)
}
