package org.xyro.kumulus

import org.xyro.kumulus.component.KumulusMessage
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

class ExecutionPool(
    size: Int,
    private val threadFun: (KumulusMessage) -> Unit,
) {
    // uncapped, memory for in-flight tuples should be taken into account and factored into max-spout-pending
    private val mainQueue = LinkedBlockingQueue<KumulusMessage>()

    var maxSize = AtomicInteger(0)

    init {
        for (i in 0 until size) {
            Thread(::threadMain).apply {
                name = "KumulusThread-$i"
                isDaemon = true
                start()
            }
        }
    }

    fun enqueue(message: KumulusMessage) {
        mainQueue.put(message)
        maxSize.getAndUpdate {
            Math.max(it, mainQueue.size)
        }
    }

    private fun threadMain() {
        while (true) {
            val message = mainQueue.take()!!
            threadFun(message)
        }
    }
}
