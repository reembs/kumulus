package org.oryx.kumulus

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping
import org.apache.storm.tuple.Tuple
import org.oryx.kumulus.component.KumulusComponent

interface KumulusEmitter {
    fun emit(
            self: KumulusComponent,
            dest: GlobalStreamId,
            grouping: Grouping,
            tuple: List<Any>,
            anchors: Collection<Tuple>?
    ): MutableList<Int>
}