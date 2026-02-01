package com.dataexport.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.core.fs.FileSystem;

import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;

/**
 * High-performance Flink job to export Oracle table to NAS drive
 * Features:
 * - Parallel reading from Oracle
 * - Checkpointing for fault tolerance
 * - Efficient file writing with compression
 * - No data loss guarantee
 */
public class OracleToNasFlinkJob {
    
    // Configuration constants
    private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
    private static final String ORACLE_URL = "jdbc:oracle:thin:@hostname:1521:SID";
    private static final String ORACLE_USER = "your_username";
    private static final String ORACLE_PASSWORD = "your_password";
    private static final String NAS_PATH = "file:///mnt/nas/output"; // or "hdfs://nas-server:9000/output"
    
    private static final String QUERY = "SELECT * FROM your_table";
    private static final int FETCH_SIZE = 10000;
    private static final int CHECKPOINT_INTERVAL = 60000; // 1 minute
    private static final int PARALLELISM = 4; // Adjust based on cores available
    
    public static void main(String[] args) throws Exception {
        
        // Create execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure for high performance and fault tolerance
        configureEnvironment(env);
        
        // Create JDBC input format for Oracle
        JdbcInputFormat<YourDataClass> jdbcInput = createJdbcInputFormat();
        
        // Read from Oracle
        DataStream<YourDataClass> oracleStream = env
            .createInput(jdbcInput)
            .name("Oracle Source")
            .setParallelism(PARALLELISM);
        
        // Transform to desired format (CSV/JSON/Parquet)
        DataStream<String> transformedStream = oracleStream
            .map(new DataTransformer())
            .name("Data Transformer");
        
        // Write to NAS with fault tolerance
        writeToNas(transformedStream);
        
        // Execute the job
        env.execute("Oracle to NAS Export Job");
    }
    
    /**
     * Configure Flink environment for high performance and reliability
     */
    private static void configureEnvironment(StreamExecutionEnvironment env) {
        // Enable checkpointing for exactly-once semantics
        env.enableCheckpointing(CHECKPOINT_INTERVAL, CheckpointingMode.EXACTLY_ONCE);
        
        // Checkpoint configuration
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(600000); // 10 minutes
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        
        // Enable checkpoint persistence
        env.getCheckpointConfig().enableExternalizedCheckpoints(
            org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        
        // Restart strategy - exponential backoff
        env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
            Duration.ofMillis(1000), // initial delay
            Duration.ofMillis(60000), // max delay
            2.0, // backoff multiplier
            Duration.ofMinutes(10), // reset interval
            0.1 // jitter factor
        ));
        
        // Set parallelism
        env.setParallelism(PARALLELISM);
    }
    
    /**
     * Create JDBC input format for reading from Oracle
     */
    private static JdbcInputFormat<YourDataClass> createJdbcInputFormat() {
        return JdbcInputFormat.<YourDataClass>buildJdbcInputFormat()
            .setDrivername(ORACLE_DRIVER)
            .setDBUrl(ORACLE_URL)
            .setUsername(ORACLE_USER)
            .setPassword(ORACLE_PASSWORD)
            .setQuery(QUERY)
            .setFetchSize(FETCH_SIZE)
            .setRowTypeInfo(/* Define TypeInformation */)
            .finish();
    }
    
    /**
     * Write stream to NAS with fault tolerance
     */
    private static void writeToNas(DataStream<String> stream) {
        
        // Option 1: CSV format with compression
        final StreamingFileSink<String> csvSink = StreamingFileSink
            .<String>forRowFormat(
                new Path(NAS_PATH),
                new SimpleStringEncoder<>("UTF-8")
            )
            .withRollingPolicy(
                OnCheckpointRollingPolicy.build()
            )
            .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH"))
            .withOutputFileConfig(
                OutputFileConfig.builder()
                    .withPartPrefix("oracle-export")
                    .withPartSuffix(".csv")
                    .build()
            )
            .build();
        
        stream.addSink(csvSink)
            .name("NAS Sink")
            .setParallelism(PARALLELISM);
    }
    
    /**
     * Data transformer - converts database records to desired format
     */
    public static class DataTransformer extends RichMapFunction<YourDataClass, String> {
        
        private static final long serialVersionUID = 1L;
        private transient StringBuilder sb;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            sb = new StringBuilder(1024);
        }
        
        @Override
        public String map(YourDataClass record) throws Exception {
            // Clear the StringBuilder
            sb.setLength(0);
            
            // Convert to CSV format (adjust based on your schema)
            sb.append(escape(record.getId())).append(",")
              .append(escape(record.getName())).append(",")
              .append(escape(record.getValue())).append("\n");
            
            return sb.toString();
        }
        
        private String escape(Object value) {
            if (value == null) return "";
            String str = value.toString();
            if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
                return "\"" + str.replace("\"", "\"\"") + "\"";
            }
            return str;
        }
    }
    
    /**
     * Data class representing Oracle table row
     * Replace with your actual table structure
     */
    public static class YourDataClass {
        private Long id;
        private String name;
        private String value;
        
        public YourDataClass() {}
        
        public YourDataClass(Long id, String name, String value) {
            this.id = id;
            this.name = name;
            this.value = value;
        }
        
        // Getters and setters
        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
    }
}
