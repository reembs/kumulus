# kumulus
A drop-in, non-distributed, replacement for Storm in Kotlin aimed for low latency requirements

Use by initializing a regular Storm topology via ```org.apache.storm.topology.TopologyBuilder``` and produce a ```StormTopology``` object. Use both to transform the topology into a `KumulusTopology`, and run it in process.

```
val kumulusTopology = KumulusStormTransformer.initializeTopology(builder, topology, config, stormId)
kumulusTopology.prepare()
kumulusTopology.start()
```