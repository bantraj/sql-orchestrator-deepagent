package com.dataexport.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcParameterValuesProvider;
import org.apache.flink.connector.jdbc.split.JdbcNumericBetweenParametersProvider;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Optimized version with parallel partitioned reading
 * This splits the Oracle table into ranges and reads in parallel
 */
public class OptimizedOracleToNasJob {
    
    private static final String ORACLE_URL = "jdbc:oracle:thin:@hostname:1521:SID";
    private static final String ORACLE_USER = "your_username";
    private static final String ORACLE_PASSWORD = "your_password";
    private static final String TABLE_NAME = "YOUR_TABLE";
    private static final String NAS_PATH = "file:///mnt/nas/output";
    
    public static void main(String[] args) throws Exception {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure environment
        env.enableCheckpointing(60000);
        env.setParallelism(8);
        
        // Read from Oracle with parallel partitioning
        DataStream<Row> oracleData = readOracleParallel(env);
        
        // Transform and write
        oracleData
            .map(new RowToCsvMapper())
            .addSink(createFileSink())
            .name("NAS Writer");
        
        env.execute("Optimized Oracle to NAS");
    }
    
    /**
     * Read Oracle table in parallel using ID range partitioning
     */
    private static DataStream<Row> readOracleParallel(StreamExecutionEnvironment env) {
        
        // Define the query with parameter placeholders
        String parameterizedQuery = "SELECT * FROM " + TABLE_NAME + 
                                   " WHERE id >= ? AND id < ?";
        
        // Define the range of IDs to partition
        // These should be determined from your actual data
        long minId = 1;
        long maxId = 10000000; // 10 million records
        int numPartitions = 8; // Number of parallel readers
        
        // Create parameters provider for range partitioning
        Serializable[][] parameters = new Serializable[numPartitions][2];
        long rangeSize = (maxId - minId) / numPartitions;
        
        for (int i = 0; i < numPartitions; i++) {
            long start = minId + (i * rangeSize);
            long end = (i == numPartitions - 1) ? maxId + 1 : start + rangeSize;
            parameters[i] = new Serializable[]{start, end};
        }
        
        // Define row type info based on your table schema
        RowTypeInfo rowTypeInfo = new RowTypeInfo(
            TypeInformation.of(Long.class),      // id
            TypeInformation.of(String.class),    // name
            TypeInformation.of(String.class)     // value
            // Add more fields as needed
        );
        
        JdbcParameterValuesProvider paramProvider = 
            new JdbcParameterValuesProvider() {
                @Override
                public Serializable[][] getParameterValues() {
                    return parameters;
                }
            };
        
        return env.createInput(
            org.apache.flink.connector.jdbc.JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("oracle.jdbc.driver.OracleDriver")
                .setDBUrl(ORACLE_URL)
                .setUsername(ORACLE_USER)
                .setPassword(ORACLE_PASSWORD)
                .setQuery(parameterizedQuery)
                .setRowTypeInfo(rowTypeInfo)
                .setFetchSize(10000)
                .setParametersProvider(paramProvider)
                .finish()
        ).setParallelism(numPartitions);
    }
    
    /**
     * Create file sink with proper configuration
     */
    private static org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink<String> createFileSink() {
        return org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
            .<String>forRowFormat(
                new org.apache.flink.core.fs.Path(NAS_PATH),
                new org.apache.flink.api.common.serialization.SimpleStringEncoder<>("UTF-8")
            )
            .withRollingPolicy(
                org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy.build()
            )
            .withOutputFileConfig(
                org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig.builder()
                    .withPartPrefix("data")
                    .withPartSuffix(".csv")
                    .build()
            )
            .build();
    }
    
    /**
     * Convert Row to CSV string
     */
    public static class RowToCsvMapper implements MapFunction<Row, String> {
        @Override
        public String map(Row row) throws Exception {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < row.getArity(); i++) {
                if (i > 0) sb.append(",");
                Object value = row.getField(i);
                sb.append(value != null ? escapeCSV(value.toString()) : "");
            }
            sb.append("\n");
            return sb.toString();
        }
        
        private String escapeCSV(String value) {
            if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
                return "\"" + value.replace("\"", "\"\"") + "\"";
            }
            return value;
        }
    }
}
