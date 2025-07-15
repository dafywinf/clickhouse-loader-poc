package com.dafywinf.clickhouse.kafka;

import com.dafywinf.clickhouse.record.ExampleRecord;
import com.dafywinf.clickhouse.record.ExampleRecordGenerator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ExampleRecordProducer {

    private final ExampleRecordGenerator generator;
    private final KafkaTemplate<String, ExampleRecord> kafkaTemplate;
    private final String topic;

    public ExampleRecordProducer(KafkaTemplate<String, ExampleRecord> kafkaTemplate,
                       @Value("${kafka.topic.ddr}") String topic,
                                 ExampleRecordGenerator generator) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.generator = generator;
    }

    public void sendSampleMessages(int count) {
        for (int i = 0; i < count; i++) {
            ExampleRecord record = generator.generateRecord(i);
            kafkaTemplate.send(topic, record.getId(), record);
        }
    }

}
