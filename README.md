[![Build Status](https://travis-ci.org/reembs/kumulus.svg?branch=master "Build Status")](https://travis-ci.org/reembs/kumulus)

# kumulus
A drop-in replacement for Apache Storm. Aimed for a sparse processing topology with low latency requirements.

What Kumulus isn't: a streaming engine, a distributed system.

In comparison to Storm, Kumulus is optimized for low end-to-end latency rather than high throughput.

As opposed to Storm that creates at least a single thread per bolt, Kumulus uses a constant size thread pool.

Possible scenarios to use Kumulus:
- You think you would need to use Storm in the future, but don't have a way to justify the kind of cluster it takes to use Storm today.
- You have huge topologies that can't be run on developer machines and are looking for a way to achieve that
- You have low latency requirements and don't imagine needing to shard your topology across multiple machines

## Usage

Include via maven
```xml
<dependency>
    <groupId>org.xyro</groupId>
    <artifactId>kumulus</artifactId>
    <version>0.1.39</version>
</dependency>
```

Use by initializing a regular Storm topology via ```org.apache.storm.topology.TopologyBuilder``` and produce a ```StormTopology``` object. Use it to transform the topology into a `KumulusTopology`, and run it in-process.

```kotlin
val builder = org.apache.storm.topology.TopologyBuilder()

val config: MutableMap<String, Any> = mutableMapOf()

builder.setSpout("spout", Spout())
builder.setBolt("bolt", Bolt())
        .shuffleGrouping("spout")

val kumulusTopology = KumulusStormTransformer.initializeTopology(
        builder.createTopology(), config, "topology_name")
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
            builder.createTopology(), config, "topology_name");
kumulusTopology.prepare();
kumulusTopology.start(true);
```

## Benchmark

Latency histograms produced by passing 100,000 (10% warm-up) tiny tuples into the fairly simple topology defined in KumulusStormTransformerTest:

*Storm 1.0.4*
```
5.0 (4512): 0.081ms
25.0 (22515): 0.126ms
50.0 (45501): 0.172ms
75.0 (67549): 0.211ms
90.0 (81003): 0.246ms
95.0 (85555): 0.269ms
98.0 (88203): 0.297ms
99.0 (89114): 0.321ms
99.9 (89910): 0.721ms
99.99 (89991): 5.035ms

Processed 100000 end-to-end messages in 109097ms
```

*Kumulus*
```
5.0 (16148): 0.01ms
25.0 (28869): 0.011ms
50.0 (53048): 0.014ms
75.0 (68302): 0.018ms
90.0 (81416): 0.029ms
95.0 (85758): 0.037ms
98.0 (88673): 0.04ms
99.0 (89207): 0.043ms
99.9 (89913): 0.064ms
99.99 (89991): 0.281ms

Processed 100000 end-to-end messages in 2813ms
```

Startup time was not included in the test results. Naturally, Storm's startup times are significantly higher in local-cluster mode.

## Storm Version

At the moment, Kumulus uses Apache Storm 1.2.2. This limits the ability to use Kumulus to projects with Storm versions < 2.

Feel free to open an issue if you require this upgrade.

## License

Licensed under Apache 2.0
