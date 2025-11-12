package org.xyro.kumulus.component

/**
 * Implementing this interface by a spout means that the spout will be notified on spout message failures.
 * Report includes the failed bolts. This interface is mutually exclusive with KumulusTimeoutNotificationSpout
 * @see KumulusTimeoutNotificationSpout
 */
interface KumulusFailureNotificationSpout {
    /**
     * Spout fail hook that includes a list of failed bolts (bolts that did not failed the msgId instead of acking)
     */
    fun messageIdFailure(msgId: Any?, failedComponents: List<String>)
}
