// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/step/groundtruth/GroundTruthConsensusItemReader.java
package com.research.hbx_invoice_entity_extraction_batch.batch.step.groundtruth;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStream;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.database.JdbcCursorItemReader;

import javax.sql.DataSource;

@Slf4j
@StepScope
public class GroundTruthConsensusItemReader implements ItemReader<String>, ItemStream {

    private static final String SQL = """
            SELECT DISTINCT invoice_id
            FROM extraction_run
            WHERE extraction_status = 'COMPLETED'
              AND invoice_id NOT IN (
                SELECT invoice_id FROM ground_truth_consensus
              )
            ORDER BY invoice_id
            """;

    private final JdbcCursorItemReader<String> delegate;

    public GroundTruthConsensusItemReader(DataSource dataSource) {
        this.delegate = new JdbcCursorItemReader<>(dataSource, SQL, (rs, rowNum) -> rs.getString(1));
        this.delegate.setName("groundTruthConsensusJdbcCursorItemReader");
        this.delegate.setSaveState(true);
    }

    @Override
    public String read() throws Exception {
        return delegate.read();
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        delegate.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        delegate.update(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        delegate.close();
    }
}
