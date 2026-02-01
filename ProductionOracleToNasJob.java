package com.dataexport.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.Serializable;
import java.sql.*;
import java.time.Duration;
import java.util.Properties;

/**
 * Production-Ready Oracle to NAS Export Job
 * 
 * Features:
 * - Configuration-driven
 * - Proper error handling and logging
 * - Metrics and monitoring
 * - Resource optimization
 * - Fault tolerance with exactly-once semantics
 */
public class ProductionOracleToNasJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(ProductionOracleToNasJob.class);
    
    public static void main(String[] args) throws Exception {
        
        // Load configuration
        String configPath = args.length > 0 ? args[0] : "application.properties";
        Properties config = loadConfiguration(configPath);
        
        LOG.info("Starting Oracle to NAS export job with configuration: {}", configPath);
        
        // Create execution environment
        StreamExecutionEnvironment env = createEnvironment(config);
        
        // Read data from Oracle
        DataStream<Row> oracleStream = readFromOracle(env, config);
        
        // Transform and add monitoring
        DataStream<String> transformedStream = oracleStream
            .map(new MonitoredRowMapper(config))
            .name("Transform and Monitor");
        
        // Write to NAS
        writeToNas(transformedStream, config);
        
        // Execute
        LOG.info("Submitting job to Flink cluster...");
        env.execute("Oracle to NAS Export - " + config.getProperty("oracle.table.name"));
    }
    
    /**
     * Load configuration from properties file
     */
    private static Properties loadConfiguration(String path) throws Exception {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(path)) {
            props.load(fis);
        }
        return props;
    }
    
    /**
     * Create and configure Flink execution environment
     */
    private static StreamExecutionEnvironment createEnvironment(Properties config) {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Set parallelism
        int parallelism = Integer.parseInt(config.getProperty("flink.parallelism", "4"));
        env.setParallelism(parallelism);
        
        // Enable checkpointing
        long checkpointInterval = Long.parseLong(config.getProperty("checkpoint.interval", "60000"));
        env.enableCheckpointing(checkpointInterval);
        
        // Checkpoint configuration
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(
            Long.parseLong(config.getProperty("checkpoint.min.pause", "30000"))
        );
        env.getCheckpointConfig().setCheckpointTimeout(
            Long.parseLong(config.getProperty("checkpoint.timeout", "600000"))
        );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(
            Integer.parseInt(config.getProperty("checkpoint.max.concurrent", "1"))
        );
        
        // Enable externalized checkpoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(
            org.apache.flink.streaming.api.environment.CheckpointConfig
                .ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        
        // Set checkpoint storage
        String checkpointPath = config.getProperty("checkpoint.storage", "file:///tmp/checkpoints");
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
        
        // Restart strategy
        env.setRestartStrategy(
            org.apache.flink.api.common.restartstrategy.RestartStrategies.exponentialDelayRestart(
                Duration.ofMillis(Long.parseLong(config.getProperty("restart.initial.delay", "1000"))),
                Duration.ofMillis(Long.parseLong(config.getProperty("restart.max.delay", "60000"))),
                Double.parseDouble(config.getProperty("restart.backoff.multiplier", "2.0")),
                Duration.ofMinutes(10),
                0.1
            )
        );
        
        LOG.info("Environment configured with parallelism: {}", parallelism);
        return env;
    }
    
    /**
     * Read data from Oracle with partitioning for parallel execution
     */
    private static DataStream<Row> readFromOracle(StreamExecutionEnvironment env, Properties config) {
        
        String url = config.getProperty("oracle.url");
        String username = config.getProperty("oracle.username");
        String password = config.getProperty("oracle.password");
        String tableName = config.getProperty("oracle.table.name");
        String partitionColumn = config.getProperty("oracle.partition.column", "ID");
        
        int partitionCount = Integer.parseInt(config.getProperty("oracle.partition.count", "8"));
        long minId = Long.parseLong(config.getProperty("oracle.partition.min", "1"));
        long maxId = Long.parseLong(config.getProperty("oracle.partition.max", "10000000"));
        
        // Create partitioned readers
        Serializable[][] partitions = createPartitions(minId, maxId, partitionCount);
        
        String query = String.format("SELECT * FROM %s WHERE %s >= ? AND %s < ?", 
                                     tableName, partitionColumn, partitionColumn);
        
        LOG.info("Reading from Oracle table: {} with {} partitions", tableName, partitionCount);
        
        JdbcInputFormat<Row> jdbcInput = JdbcInputFormat.buildJdbcInputFormat()
            .setDrivername(config.getProperty("oracle.driver"))
            .setDBUrl(url)
            .setUsername(username)
            .setPassword(password)
            .setQuery(query)
            .setFetchSize(Integer.parseInt(config.getProperty("jdbc.fetch.size", "10000")))
            .setRowTypeInfo(/* Define based on your schema */)
            .setParametersProvider(new SimpleParameterProvider(partitions))
            .finish();
        
        return env.createInput(jdbcInput)
            .name("Oracle Source: " + tableName)
            .setParallelism(partitionCount);
    }
    
    /**
     * Create partition ranges for parallel reading
     */
    private static Serializable[][] createPartitions(long min, long max, int count) {
        Serializable[][] partitions = new Serializable[count][2];
        long rangeSize = (max - min) / count;
        
        for (int i = 0; i < count; i++) {
            long start = min + (i * rangeSize);
            long end = (i == count - 1) ? max + 1 : start + rangeSize;
            partitions[i] = new Serializable[]{start, end};
            LOG.debug("Partition {}: {} to {}", i, start, end);
        }
        
        return partitions;
    }
    
    /**
     * Write to NAS with proper file management
     */
    private static void writeToNas(DataStream<String> stream, Properties config) {
        
        String outputPath = config.getProperty("nas.output.path");
        String filePrefix = config.getProperty("output.file.prefix", "data");
        String fileSuffix = config.getProperty("output.file.suffix", ".csv");
        
        LOG.info("Writing output to: {}", outputPath);
        
        FileSink<String> sink = FileSink
            .<String>forRowFormat(new Path(outputPath), new SimpleStringEncoder<>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofMinutes(15))
                    .withInactivityInterval(Duration.ofMinutes(5))
                    .withMaxPartSize(512 * 1024 * 1024L) // 512 MB
                    .build()
            )
            .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd/HH"))
            .withOutputFileConfig(
                OutputFileConfig.builder()
                    .withPartPrefix(filePrefix)
                    .withPartSuffix(fileSuffix)
                    .build()
            )
            .build();
        
        stream.sinkTo(sink)
            .name("NAS File Sink")
            .setParallelism(Integer.parseInt(config.getProperty("flink.parallelism", "4")));
    }
    
    /**
     * Mapper with monitoring and metrics
     */
    public static class MonitoredRowMapper extends RichMapFunction<Row, String> {
        
        private static final Logger LOG = LoggerFactory.getLogger(MonitoredRowMapper.class);
        
        private final Properties config;
        private transient Counter recordCounter;
        private transient Counter errorCounter;
        private transient StringBuilder sb;
        
        public MonitoredRowMapper(Properties config) {
            this.config = config;
        }
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            
            // Initialize metrics
            this.recordCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("records_processed");
            
            this.errorCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("errors");
            
            this.sb = new StringBuilder(2048);
            
            LOG.info("Mapper initialized on task: {}", getRuntimeContext().getIndexOfThisSubtask());
        }
        
        @Override
        public String map(Row row) throws Exception {
            try {
                sb.setLength(0);
                
                // Convert row to CSV
                for (int i = 0; i < row.getArity(); i++) {
                    if (i > 0) sb.append(",");
                    Object field = row.getField(i);
                    sb.append(escapeCSV(field));
                }
                sb.append("\n");
                
                recordCounter.inc();
                
                return sb.toString();
                
            } catch (Exception e) {
                errorCounter.inc();
                LOG.error("Error processing row: {}", row, e);
                throw e;
            }
        }
        
        private String escapeCSV(Object value) {
            if (value == null) return "";
            
            String str = value.toString();
            if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
                return "\"" + str.replace("\"", "\"\"") + "\"";
            }
            return str;
        }
        
        @Override
        public void close() throws Exception {
            super.close();
            LOG.info("Mapper closed. Total records processed: {}", recordCounter.getCount());
        }
    }
    
    /**
     * Simple parameter provider for JDBC partitioning
     */
    public static class SimpleParameterProvider implements org.apache.flink.connector.jdbc.JdbcParameterValuesProvider {
        
        private final Serializable[][] parameters;
        
        public SimpleParameterProvider(Serializable[][] parameters) {
            this.parameters = parameters;
        }
        
        @Override
        public Serializable[][] getParameterValues() {
            return parameters;
        }
    }
}
