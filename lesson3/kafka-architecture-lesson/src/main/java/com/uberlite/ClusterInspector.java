package com.uberlite;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Inspects Kafka cluster topology and partition distribution.
 */
public class ClusterInspector {
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, 
            "localhost:9092,localhost:9093,localhost:9094");
        
        try (var admin = AdminClient.create(props)) {
            
            System.out.println("=== Cluster Nodes ===");
            var nodes = admin.describeCluster().nodes().get();
            nodes.forEach(node -> 
                System.out.printf("Broker %d: %s:%d%n", 
                    node.id(), node.host(), node.port()));
            
            System.out.println("\n=== Topic: driver-locations ===");
            var topicDesc = admin.describeTopics(List.of("driver-locations"))
                .allTopicNames().get();
            
            var topic = topicDesc.get("driver-locations");
            System.out.println("Partitions: " + topic.partitions().size());
            
            System.out.println("\nPartition Distribution:");
            for (TopicPartitionInfo partition : topic.partitions()) {
                var leader = partition.leader().id();
                var replicas = partition.replicas().stream()
                    .map(node -> String.valueOf(node.id()))
                    .toList();
                var isr = partition.isr().stream()
                    .map(node -> String.valueOf(node.id()))
                    .toList();
                
                System.out.printf("  Partition %d: Leader=%d, Replicas=%s, ISR=%s%n",
                    partition.partition(), leader, replicas, isr);
                
                if (isr.size() < replicas.size()) {
                    System.out.println("    WARNING: Under-replicated!");
                }
            }
        }
    }
}
