# kumulus
A drop-in, non-distributed, replacement for Storm in Kotlin aimed for low latency requirements

Use by initializing a regular Storm topology via ```org.apache.storm.topology.TopologyBuilder``` and produce a ```StormTopology``` object. Use both to transform the topology into a `KumulusTopology`, and run it in process.

```kotlin
val builder = org.apache.storm.topology.TopologyBuilder()

val config: MutableMap<String, Any> = mutableMapOf()

builder.setSpout("spout", Spout())
builder.setBolt("bolt", Bolt()).shuffleGrouping("spout")

val kumulusTopology = KumulusStormTransformer.initializeTopology(builder, topology, config, "topology_name")
kumulusTopology.prepare()
kumulusTopology.start()
```

Latency histograms produced by passing 10,000 tiny tuples into the fairly simple topology defined in KumulusStormTransformerTest:

*Storm 1.0.4*
```
5.0 (514): 0.117ms
25.0 (2558): 0.148ms
50.0 (5052): 0.177ms
75.0 (7514): 0.217ms
90.0 (9014): 0.264ms
95.0 (9504): 0.333ms
98.0 (9800): 0.476ms
99.0 (9900): 0.585ms
99.9 (9990): 1.141ms
99.99 (9999): 5.397ms

took: 13162ms
```

*Kumulus*
```
5.0 (1227): 0.044ms
25.0 (2837): 0.046ms
50.0 (5050): 0.062ms
75.0 (7590): 0.089ms
90.0 (9042): 0.123ms
95.0 (9517): 0.141ms
98.0 (9801): 0.175ms
99.0 (9900): 0.214ms
99.9 (9990): 0.427ms
99.99 (9999): 4.448ms

took: 904ms
```

Startup time was not included in the test results. Naturally, Storm's startup times are significantly higher.