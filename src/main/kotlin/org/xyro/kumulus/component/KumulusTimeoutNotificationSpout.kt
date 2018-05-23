package org.xyro.kumulus.component

/**
 * Implementing this interface by a spout means that the spout will be notified on spout message timeouts.
 * Report includes the timeout bolts.
 * This interface is mutually exclusive with KumulusFailureNotificationSpout
 * @see KumulusFailureNotificationSpout
 */
interface KumulusTimeoutNotificationSpout {
    /**
     * Spout fail hook that includes a list of timeout bolts (bolts that did not ack/fail the msgId)
     */
    fun messageIdFailure(msgId: Any?, failedComponents: List<String>, timeoutComponents: List<String>)
}