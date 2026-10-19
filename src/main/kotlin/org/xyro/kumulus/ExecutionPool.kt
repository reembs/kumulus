package org.xyro.kumulus

import java.util.concurrent.Executor
import java.util.concurrent.LinkedTransferQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * [Executor] decorator that tracks how many submitted tasks have not started yet, and the peak of that count.
 */
class ExecutionPool(
    private val delegate: Executor,
) : Executor {
    private val queueSize = AtomicInteger(0)
    private val maxSize = AtomicInteger(0)

    /** Peak number of submitted-but-not-started tasks since construction or the last [resetMaxQueueSize]. */
    val maxQueueSize: Int
        get() = maxSize.get()

    fun resetMaxQueueSize() {
        maxSize.set(0)
    }

    override fun execute(command: Runnable) {
        val currentSize = queueSize.incrementAndGet()
        maxSize.getAndUpdate {
            Math.max(it, currentSize)
        }
        try {
            delegate.execute {
                queueSize.decrementAndGet()
                command.run()
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
