package org.oryx.kumulus

import org.apache.storm.generated.GlobalStreamId
import org.apache.storm.generated.Grouping

interface KumulusEmitter {
    fun emit(dest: GlobalStreamId, grouping: Grouping, tuple: MutableList<Any>?) : MutableList<Int>
}