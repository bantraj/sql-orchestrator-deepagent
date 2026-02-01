====
public class ColumnRangePartitioner implements Partitioner {
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        // In a real scenario, you'd query the DB for min/max ID
        int min = 1;
        int max = 10000; 
        int targetSize = (max - min) / gridSize + 1;

        Map<String, ExecutionContext> result = new HashMap<>();
        int number = 0;
        int start = min;
        int end = start + targetSize - 1;

        while (start <= max) {
            ExecutionContext value = new ExecutionContext();
            value.putInt("minValue", start);
            value.putInt("maxValue", end);
            result.put("partition" + number, value);
            start += targetSize;
            end += targetSize;
            number++;
        }
        return result;
    }
}

====

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private DataSource dataSource;

    // --- SLAVE STEP COMPONENTS ---

    @Bean
    @StepScope
    public JdbcPagingItemReader<User> reader(
            @Value("#{stepExecutionContext['minValue']}") Long minValue,
            @Value("#{stepExecutionContext['maxValue']}") Long maxValue) {
        
        JdbcPagingItemReader<User> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setFetchSize(1000);
        reader.setRowMapper(new BeanPropertyRowMapper<>(User.class));

        OraclePagingQueryProvider queryProvider = new OraclePagingQueryProvider();
        queryProvider.setSelectClause("SELECT id, name, email");
        queryProvider.setFromClause("FROM users");
        queryProvider.setWhereClause("WHERE id >= " + minValue + " AND id <= " + maxValue);
        queryProvider.setSortKeys(Collections.singletonMap("id", Order.ASCENDING));

        reader.setQueryProvider(queryProvider);
        return reader;
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<User> writer(
            @Value("#{stepExecutionContext['partition0']}") String name) {
        FlatFileItemWriter<User> writer = new FlatFileItemWriter<>();
        // Unique filename per partition to avoid write contention
        writer.setResource(new FileSystemResource("output/users_part_" + name + ".csv"));
        writer.setLineAggregator(new DelimitedLineAggregator<User>() {{
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor<User>() {{
                setNames(new String[]{"id", "name", "email"});
            }});
        }});
        return writer;
    }

    // --- STEP & JOB DEFINITIONS ---

    @Bean
    public Step slaveStep() {
        return stepBuilderFactory.get("slaveStep")
                .<User, User>chunk(500)
                .reader(reader(null, null))
                .writer(writer(null))
                .build();
    }

    @Bean
    public Step masterStep() {
        return stepBuilderFactory.get("masterStep")
                .partitioner(slaveStep().getName(), new ColumnRangePartitioner())
                .step(slaveStep())
                .gridSize(5) // Number of concurrent threads
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }

    @Bean
    public Job exportJob() {
        return jobBuilderFactory.get("exportJob")
                .start(masterStep())
                .build();
    }
}

===
@Configuration
public class BatchSchedulerConfig {

    @Bean
    public JobLauncher asyncJobLauncher(JobRepository jobRepository) throws Exception {
        TaskLauncherJobLauncher jobLauncher = new TaskLauncherJobLauncher();
        SimpleJobLauncher launcher = new SimpleJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.setTaskExecutor(new SimpleAsyncTaskExecutor()); // Makes it asynchronous
        launcher.afterPropertiesSet();
        return launcher;
    }
}

====
@Bean
public Step metadataStep() {
    return stepBuilderFactory.get("metadataStep")
            .tasklet((contribution, chunkContext) -> {
                JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                Map<String, Object> results = jdbcTemplate.queryForMap("SELECT MIN(id) as min, MAX(id) as max FROM users");
                
                // Pass values to the JobContext so the Partitioner can see them
                ExecutionContext jobContext = chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext();
                jobContext.putLong("minId", (Long) results.get("min"));
                jobContext.putLong("maxId", (Long) results.get("max"));
                
                return RepeatStatus.FINISHED;
            }).build();
}

@Bean
public Job exportJob() {
    return jobBuilderFactory.get("exportJob")
            .start(metadataStep()) // First, find the ranges
            .next(masterStep())    // Then, partition and process
            .build();
}
====
  <dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-batch</artifactId>
</dependency>
<dependency>
    <groupId>com.oracle.database.jdbc</groupId>
    <artifactId>ojdbc8</artifactId>
    <scope>runtime</scope>
</dependency>

  ====


  plugins {
    id 'java'
    id 'org.springframework.boot' version '3.2.0'
    id 'io.spring.dependency-management' version '1.1.4'
}

group = 'com.example'
version = '0.0.1-SNAPSHOT'

java {
    sourceCompatibility = '17' // Spring Boot 3 requires Java 17+
}

repositories {
    mavenCentral()
}

dependencies {
    // Spring Boot Starters
    implementation 'org.springframework.boot:spring-boot-starter-batch'
    implementation 'org.springframework.boot:spring-boot-starter-web'
    implementation 'org.springframework.boot:spring-boot-starter-jdbc'

    // Oracle JDBC Driver
    // Use 'ojdbc11' for Java 11/17+ compatibility
    implementation 'com.oracle.database.jdbc:ojdbc11:23.3.0.23.09'

    // Optional: Lombok to reduce boilerplate for your User POJO
    compileOnly 'org.projectlombok:lombok'
    annotationProcessor 'org.projectlombok:lombok'

    // Testing
    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'org.springframework.batch:spring-batch-test'
}

tasks.named('test') {
    useJUnitPlatform()
}

===
  
