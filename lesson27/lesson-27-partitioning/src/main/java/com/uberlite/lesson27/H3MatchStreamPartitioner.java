package com.uberlite.lesson27;

import com.uberlite.lesson27.Models.MatchEvent;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Optional;
import java.util.Set;

/** StreamPartitioner for match-events sink (key=Long h3Cell, value=MatchEvent). */
public class H3MatchStreamPartitioner implements StreamPartitioner<Long, MatchEvent> {

    @Override
    @Deprecated
    public Integer partition(String topic, Long key, MatchEvent value, int numPartitions) {
        return H3Util.partitionFor(key, numPartitions);
    }

    @Override
    public Optional<Set<Integer>> partitions(String topic, Long key, MatchEvent value, int numPartitions) {
        return Optional.of(Set.of(H3Util.partitionFor(key, numPartitions)));
    }
}
