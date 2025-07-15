package com.dafywinf.clickhouse.record;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class ExampleRecordGenerator {
    public ExampleRecord generateRecord(int index) {
        return new ExampleRecord(
                UUID.randomUUID().toString(),
                Instant.now(),
                "value-" + index
        );
    }

    public List<ExampleRecord> generateBatch(int size) {
        List<ExampleRecord> batch = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            batch.add(generateRecord(i));
        }
        return batch;
    }
}