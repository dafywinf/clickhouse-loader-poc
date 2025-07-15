package com.dafywinf.clickhouse;

import com.dafywinf.clickhouse.jdbc.JdbcRecordService;
import com.dafywinf.clickhouse.record.ExampleRecord;
import com.dafywinf.clickhouse.record.ExampleRecordGenerator;
import com.dafywinf.clickhouse.kafka.ExampleRecordProducer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


@SpringBootApplication
public class SpringRecordProducerApplication {

    private static final int THREAD_COUNT = 10;
    private static final int BATCH_COUNT = 100;
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) {
        SpringApplication.run(SpringRecordProducerApplication.class, args);
    }

    @Bean
    @ConditionalOnProperty(name = "loader.mode", havingValue = "kafka", matchIfMissing = false)
    CommandLineRunner run(ExampleRecordProducer producer) {
        return args -> {
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            AtomicLong totalMessagesSent = new AtomicLong(0);
            long start = System.nanoTime();

            for (int i = 0; i < BATCH_COUNT; i++) {
                final int batchIndex = i;
                executor.submit(() -> {
                    try {
                        producer.sendSampleMessages(BATCH_SIZE);
                        long newTotal = totalMessagesSent.addAndGet(BATCH_SIZE);
                        System.out.printf("Batch %,d complete. Total messages sent so far: %,d%n", batchIndex, newTotal);
                    } catch (Exception e) {
                        System.err.printf("Error in batch %,d: %s%n", batchIndex, e.getMessage());
                        e.printStackTrace();
                    }
                });
            }

            executor.shutdown();
            if (executor.awaitTermination(10, TimeUnit.MINUTES)) {
                long duration = System.nanoTime() - start;
                System.out.printf("All batches complete. Total messages sent: %,d%n", totalMessagesSent.get());
                System.out.printf("Time taken: %.2f seconds%n", duration / 1_000_000_000.0);
            } else {
                System.err.println("Timed out waiting for all threads to complete.");
            }
        };
    }

    @Bean
    @ConditionalOnProperty(name = "loader.mode", havingValue = "jdbc", matchIfMissing = false)
    CommandLineRunner clickhouseRunner(JdbcRecordService recordService, ExampleRecordGenerator recordGenerator) {
        return args -> {
            ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            AtomicLong totalWritten = new AtomicLong(0);
            long start = System.nanoTime();

            for (int i = 0; i < BATCH_COUNT; i++) {
                final int batchIndex = i;
                executor.submit(() -> {
                    try {
                        List<ExampleRecord> batch = recordGenerator.generateBatch(BATCH_SIZE);
                        recordService.writeBatch(batch);
                        long newTotal = totalWritten.addAndGet(batch.size());
                        System.out.printf("Batch %,d written. Total records so far: %,d%n", batchIndex, newTotal);
                    } catch (Exception e) {
                        System.err.printf("Error in batch %,d: %s%n", batchIndex, e.getMessage());
                        e.printStackTrace();
                    }
                });
            }

            executor.shutdown();
            if (executor.awaitTermination(10, TimeUnit.MINUTES)) {
                long duration = System.nanoTime() - start;
                System.out.printf("All batches complete. Total records written: %,d%n", totalWritten.get());
                System.out.printf("Time taken: %.2f seconds%n", duration / 1_000_000_000.0);
            } else {
                System.err.println("Timed out waiting for threads.");
            }
        };
    }
}


