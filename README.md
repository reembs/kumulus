[![Build Status](https://travis-ci.org/reembs/kumulus.svg?branch=master "Build Status")](https://travis-ci.org/reembs/kumulus)

# kumulus
A drop-in, non-distributed, replacement for Storm in Kotlin aimed for low latency requirements

Use by initializing a regular Storm topology via ```org.apache.storm.topology.TopologyBuilder``` and produce a ```StormTopology``` object. Use both to transform the topology into a `KumulusTopology`, and run it in process.

```kotlin
val builder = org.apache.storm.topology.TopologyBuilder()

val config: MutableMap<String, Any> = mutableMapOf()

builder.setSpout("spout", Spout())
builder.setBolt("bolt", Bolt())
        .shuffleGrouping("spout")

val kumulusTopology = KumulusStormTransformer.initializeTopology(
        builder, builder.createTopology(), config, "topology_name")
kumulusTopology.prepare()
kumulusTopology.start(true)
```

Or from Java:
```java
TopologyBuilder builder = new TopologyBuilder();

Map<String, Object> config = new HashMap<String, Object>();

builder.setSpout("spout", new Spout());
builder.setBolt("bolt", new Bolt())
        .shuffleGrouping("spout");

KumulusTopology kumulusTopology =
        KumulusStormTransformer.initializeTopology(
            builder, builder.createTopology(), config, "topology_name");
kumulusTopology.prepare();
kumulusTopology.start(true);
```

Latency histograms produced by passing 100,000 (10% warm-up) tiny tuples into the fairly simple topology defined in KumulusStormTransformerTest:

*Storm 1.0.4*
```
5.0 (4596): 0.116ms
25.0 (22748): 0.149ms
50.0 (45049): 0.179ms
75.0 (67824): 0.226ms
90.0 (81012): 0.275ms
95.0 (85507): 0.353ms
98.0 (88205): 0.486ms
99.0 (89103): 0.561ms
99.9 (89910): 0.819ms
99.99 (89991): 4.726ms

Done, took: 131152ms
```

*Kumulus*
```
5.0 (8995): 0.038ms
25.0 (30763): 0.039ms
50.0 (52275): 0.04ms
75.0 (68852): 0.043ms
90.0 (81490): 0.059ms
95.0 (85663): 0.072ms
98.0 (88242): 0.088ms
99.0 (89112): 0.103ms
99.9 (89910): 0.165ms
99.99 (89991): 1.06ms

Done, took: 5874ms
```

Startup time was not included in the test results. Naturally, Storm's startup times are significantly higher.
