package org.xyro.kumulus

import org.apache.storm.topology.TopologyBuilder
import org.junit.Assert.fail
import org.junit.Test
import org.xyro.kumulus.topology.KumulusTopologyBuilder
import java.lang.reflect.Modifier

class KumulusTopologyBuilderApiParityTest {
    @Test
    fun testPublicApiMatchesStormTopologyBuilder() {
        val stormApi = publicInstanceMethodSignatures(TopologyBuilder::class.java)
        val nativeApi = publicInstanceMethodSignatures(KumulusTopologyBuilder::class.java)

        val missingFromNative = stormApi - nativeApi
        val extraInNative = nativeApi - stormApi

        if (missingFromNative.isNotEmpty() || extraInNative.isNotEmpty()) {
            val message = buildString {
                appendLine("KumulusTopologyBuilder API mismatch against Storm TopologyBuilder.")
                appendLine("Missing in native builder:")
                appendLine(formatSignatureList(missingFromNative))
                appendLine("Extra in native builder:")
                appendLine(formatSignatureList(extraInNative))
            }
            fail(message)
        }
    }

    private fun publicInstanceMethodSignatures(clazz: Class<*>): Set<String> {
        return clazz.declaredMethods
            .filter { Modifier.isPublic(it.modifiers) && !Modifier.isStatic(it.modifiers) }
            .filterNot { it.isSynthetic || it.isBridge }
            .map { method ->
                val parameterTypes = method.parameterTypes.joinToString(",") { it.name }
                "${method.name}($parameterTypes)"
            }
            .toSet()
    }

    private fun formatSignatureList(signatures: Set<String>): String {
        if (signatures.isEmpty()) {
            return "  (none)"
        }
        return signatures.sorted().joinToString(separator = "\n") { "  $it" }
    }
}
