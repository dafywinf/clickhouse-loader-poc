package com.dafywinf.clickhouse.record;

import java.time.Instant;

public class ExampleRecord {
    private String id;
    private Instant timestamp;
    private String value;

    public ExampleRecord() {}

    public ExampleRecord(String id, Instant timestamp, String value) {
        this.id = id;
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }
}
