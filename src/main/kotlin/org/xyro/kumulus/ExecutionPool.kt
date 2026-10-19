package org.xyro.kumulus

import org.xyro.kumulus.component.KumulusMessage
import java.util.concurrent.Executor
import java.util.concurrent.LinkedTransferQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ExecutionPool(
    private val executor: Executor,
    private val threadFun: (KumulusMessage) -> Unit,
) {
    private val queueSize = AtomicInteger(0)

    var maxSize = AtomicInteger(0)

    fun enqueue(message: KumulusMessage) {
        val currentSize = queueSize.incrementAndGet()
        maxSize.getAndUpdate {
            Math.max(it, currentSize)
        }
        try {
            executor.execute {
                queueSize.decrementAndGet()
                threadFun(message)
            }
        } catch (e: Exception) {
            queueSize.decrementAndGet()
            throw e
        }
    }

    companion object {
        /**
         * Fixed pool of [size] daemon threads named "KumulusThread-N".
         */
        fun defaultExecutor(size: Int): ThreadPoolExecutor {
            // TODO: replace with Thread.ofPlatform().name("KumulusThread-", 0).daemon().factory() once Java 21 is the minimum runtime
            val threadIndex = AtomicInteger(0)
            val threadFactory =
                ThreadFactory { runnable ->
                    Thread(runnable, "KumulusThread-${threadIndex.getAndIncrement()}").apply {
                        isDaemon = true
                    }
                }
            return ThreadPoolExecutor(
                size,
                size,
                0L,
                TimeUnit.MILLISECONDS,
                // uncapped, memory for in-flight tuples should be taken into account and factored into max-spout-pending
                LinkedTransferQueue(),
                threadFactory,
            ).apply {
                prestartAllCoreThreads()
            }
        }
    }
}
