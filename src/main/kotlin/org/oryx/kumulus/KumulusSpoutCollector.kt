package org.oryx.kumulus

import org.apache.storm.spout.ISpoutOutputCollector

class KumulusSpoutCollector : KumulusCollector(), ISpoutOutputCollector {
    override fun emitDirect(taskId: Int, streamId: String?, tuple: MutableList<Any>?, messageId: Any?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getPendingCount(): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun reportError(error: Throwable?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun emit(streamId: String?, tuple: MutableList<Any>?, messageId: Any?): MutableList<Int> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}