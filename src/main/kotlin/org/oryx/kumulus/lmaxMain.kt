package org.oryx.kumulus

import com.lmax.disruptor.EventHandler
import com.lmax.disruptor.dsl.Disruptor
import java.nio.ByteBuffer
import java.util.concurrent.Executors
import com.lmax.disruptor.EventFactory
import com.lmax.disruptor.RingBuffer

data class LongEvent(private var value: Long = 0) {
    fun set(value: Long) {
        this.value = value
    }
}

class LongEventHandler : EventHandler<LongEvent> {
    override fun onEvent(event: LongEvent, sequence: Long, endOfBatch: Boolean) {
        println("Event: " + event)
    }
}

class LongEventFactory : EventFactory<LongEvent> {
    override fun newInstance(): LongEvent {
        return LongEvent()
    }
}

class LongEventProducer(private val ringBuffer: RingBuffer<LongEvent>) {
    fun onData(bb: ByteBuffer) {
        val sequence = ringBuffer.next()  // Grab the next sequence
        try {
            val event = ringBuffer.get(sequence) // Get the entry in the Disruptor
            // for the sequence
            event.set(bb.getLong(0))  // Fill with data
        } finally {
            ringBuffer.publish(sequence)
        }
    }
}

object LongEventMain {
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        // Executor that will be used to construct new threads for consumers
        val executor = Executors.newCachedThreadPool()

        // The factory for the event
        val factory = LongEventFactory()

        // Specify the size of the ring buffer, must be power of 2.
        val bufferSize = 1024

        // Construct the Disruptor
        val disruptor = Disruptor(factory, bufferSize, executor)

        // Connect the handler
        disruptor.handleEventsWith(LongEventHandler())

        // Start the Disruptor, starts all threads running
        disruptor.start()

        // Get the ring buffer from the Disruptor to be used for publishing.
        val ringBuffer = disruptor.getRingBuffer()

        val producer = LongEventProducer(ringBuffer)

        val bb = ByteBuffer.allocate(8)
        var l: Long = 0
        while (true) {
            bb.putLong(0, l)
            producer.onData(bb)
            Thread.sleep(1000)
            l++
        }
    }
}