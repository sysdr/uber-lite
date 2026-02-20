package com.uberlite.lesson27;

import com.uberlite.lesson27.Models.DriverLocation;
import org.apache.kafka.streams.processor.StreamPartitioner;
import java.util.Optional;
import java.util.Set;

/**
 * Kafka Streams-side StreamPartitioner (kafka-streams API).
 *
 * Intercepts Kafka Streams' .to() sink and .repartition() calls.
 * The key is the H3 cell as a Long (we re-key the stream before sinking).
 *
 * This must be registered on every sink that writes to a co-partitioned topic.
 *
 * Identity: H3Util.partitionFor(h3Cell, numPartitions)
 * This is IDENTICAL to H3ProducerPartitioner — the co-partitioning contract is upheld.
 */
public class H3StreamPartitioner implements StreamPartitioner<Long, DriverLocation> {

    @Override
    @Deprecated
    public Integer partition(String topic, Long h3Cell, DriverLocation value, int numPartitions) {
        return H3Util.partitionFor(h3Cell, numPartitions);
    }

    @Override
    public Optional<Set<Integer>> partitions(String topic, Long h3Cell,
                                              DriverLocation value, int numPartitions) {
        int partition = H3Util.partitionFor(h3Cell, numPartitions);
        return Optional.of(Set.of(partition));
    }
}
