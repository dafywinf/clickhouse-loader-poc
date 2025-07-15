package com.dafywinf.clickhouse.jdbc;

import com.dafywinf.clickhouse.record.ExampleRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

@Service
public class JdbcRecordService {

    private final JdbcTemplate jdbc;

    public JdbcRecordService(DataSource dataSource) {
        this.jdbc = new JdbcTemplate(dataSource);
    }

    public void writeBatch(List<ExampleRecord> records) {
        jdbc.batchUpdate(
                "INSERT INTO hello.example_records (id, timestamp, value) VALUES (?, ?, ?)",
                records,
                500, // batch size
                (ps, record) -> {
                    ps.setString(1, record.getId());
                    ps.setTimestamp(2, Timestamp.from(record.getTimestamp()));
                    ps.setString(3, record.getValue());
                }
        );
    }
}
