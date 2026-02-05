package com.uberlite.lesson16;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;

public record ProducerMetrics(
    double batchSizeAvg,
    double lingerTimeAvg,
    double requestRate,
    double recordSendRate,
    double compressionRate
) {
    public static ProducerMetrics capture(KafkaProducer<?, ?> producer) {
        Map<MetricName, ? extends Metric> metrics = producer.metrics();
        
        return new ProducerMetrics(
            getMetric(metrics, "batch-size-avg"),
            getMetric(metrics, "record-queue-time-avg"),
            getMetric(metrics, "request-rate"),
            getMetric(metrics, "record-send-rate"),
            getMetric(metrics, "compression-rate-avg")
        );
    }
    
    private static double getMetric(Map<MetricName, ? extends Metric> metrics, String name) {
        return metrics.entrySet().stream()
            .filter(e -> e.getKey().name().equals(name))
            .findFirst()
            .map(e -> (Double) e.getValue().metricValue())
            .orElse(0.0);
    }
    
    public void print(String label) {
        System.out.printf("""
            
            === %s ===
            Batch Size Avg:     %.0f bytes
            Linger Time Avg:    %.1f ms
            Request Rate:       %.1f req/sec
            Record Send Rate:   %.1f rec/sec
            Compression Rate:   %.2f
            """, label, batchSizeAvg, lingerTimeAvg, requestRate, recordSendRate, compressionRate);
    }
}
