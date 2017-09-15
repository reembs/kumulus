package org.oryx.kumulus

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.oryx.kumulus.component.KumulusComponent

interface KumulusEmitter {
    fun emit(
            self: KumulusComponent,
            dest: GlobalStreamId,
            grouping: Grouping,
            tuple: MutableList<Any>?
    ) : MutableList<Int>
}