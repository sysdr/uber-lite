package com.uberlite.lesson01.postgres;

import com.uberlite.lesson01.model.LocationEvent;
import com.uberlite.lesson01.model.Metrics;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class PostgresLocationUpdater {
    private final HikariDataSource dataSource;
    private final AtomicLong successCount = new AtomicLong();
    private final AtomicLong failureCount = new AtomicLong();
    private final List<Long> latencies = new ArrayList<>();

    public PostgresLocationUpdater(String jdbcUrl) {
        var config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername("postgres");
        config.setPassword("postgres");
        config.setMaximumPoolSize(100);
        config.setConnectionTimeout(5000);
        config.setIdleTimeout(30000);
        this.dataSource = new HikariDataSource(config);
        
        initSchema();
    }

    private void initSchema() {
        try (var conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS drivers (
                    driver_id VARCHAR(64) PRIMARY KEY,
                    latitude DECIMAL(10, 8),
                    longitude DECIMAL(11, 8),
                    updated_at BIGINT
                )
            """);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_updated_at ON drivers(updated_at)");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to init schema", e);
        }
    }

    public void updateLocation(LocationEvent event) {
        var startTime = System.nanoTime();
        try (var conn = dataSource.getConnection();
             var stmt = conn.prepareStatement(
                 "INSERT INTO drivers (driver_id, latitude, longitude, updated_at) " +
                 "VALUES (?, ?, ?, ?) " +
                 "ON CONFLICT (driver_id) DO UPDATE SET " +
                 "latitude = EXCLUDED.latitude, " +
                 "longitude = EXCLUDED.longitude, " +
                 "updated_at = EXCLUDED.updated_at"
             )) {
            
            stmt.setString(1, event.driverId());
            stmt.setDouble(2, event.latitude());
            stmt.setDouble(3, event.longitude());
            stmt.setLong(4, event.timestamp());
            stmt.executeUpdate();
            
            var latencyNs = System.nanoTime() - startTime;
            synchronized (latencies) {
                latencies.add(latencyNs);
            }
            successCount.incrementAndGet();
            
        } catch (SQLException e) {
            failureCount.incrementAndGet();
        }
    }

    public Metrics getMetrics() {
        List<Long> snapshot;
        synchronized (latencies) {
            snapshot = new ArrayList<>(latencies);
            latencies.clear();
        }
        
        if (snapshot.isEmpty()) {
            return new Metrics(successCount.get(), failureCount.get(), 0.0, 0.0);
        }
        
        snapshot.sort(Long::compareTo);
        var p99Index = (int) (snapshot.size() * 0.99);
        var p99LatencyMs = snapshot.get(p99Index) / 1_000_000.0;
        var throughput = successCount.get() / 10.0; // 10 second window
        
        return new Metrics(successCount.get(), failureCount.get(), p99LatencyMs, throughput);
    }

    public void close() {
        dataSource.close();
    }
}
