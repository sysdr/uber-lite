package com.uberlite.lesson27;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import java.time.Duration;
import java.util.*;

/**
 * Queries Kafka broker for per-partition offset lag and byte rates.
 * Used by verify.sh to compute partition skew and validate co-partitioning.
 * Outputs JSON-like lines for easy parsing.
 */
public class PartitionInspector {

    public static void main(String[] args) throws Exception {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "lesson27-inspector");
        props.put("key.deserializer",   "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("auto.offset.reset",  "earliest");

        try (var admin  = AdminClient.create(props);
             var consumer = new KafkaConsumer<Long, byte[]>(props)) {

            // ── Partition counts (co-partition validation) ────────────────────
            var topics = List.of(TopicBootstrap.DRIVER_TOPIC, TopicBootstrap.RIDER_TOPIC);
            var descs  = admin.describeTopics(topics).allTopicNames().get();

            descs.forEach((topic, desc) ->
                System.out.printf("PARTITION_COUNT topic=%s count=%d%n",
                    topic, desc.partitions().size()));

            // ── Per-partition end offsets (skew proxy) ───────────────────────
            var driverPartitions = new ArrayList<TopicPartition>();
            for (int i = 0; i < TopicBootstrap.PARTITIONS; i++) {
                driverPartitions.add(new TopicPartition(TopicBootstrap.DRIVER_TOPIC, i));
            }

            var endOffsets = consumer.endOffsets(driverPartitions);
            long maxOffset = 0, minOffset = Long.MAX_VALUE, totalOffset = 0;
            for (var entry : endOffsets.entrySet()) {
                long offset = entry.getValue();
                System.out.printf("PARTITION partition=%d offset=%d%n", entry.getKey().partition(), offset);
                maxOffset   = Math.max(maxOffset, offset);
                minOffset   = Math.min(minOffset, offset);
                totalOffset += offset;
            }

            double avg  = totalOffset / (double) endOffsets.size();
            double skew = (avg > 0) ? maxOffset / avg : 1.0;
            System.out.printf("SKEW max=%d min=%d avg=%.1f skew_ratio=%.2f%n",
                maxOffset, minOffset, avg, skew);
        }
    }
}
