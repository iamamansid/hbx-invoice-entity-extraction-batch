package com.research.hbx_invoice_entity_extraction_batch.batch.step.export;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class BigQueryItemWriter implements ItemWriter<InsertAllRequest.RowToInsert> {

    @Value("${gcp.project-id}")
    private String projectId;
    
    @Value("${bigquery.dataset-id}")
    private String datasetId;
    
    @Value("${bigquery.table-id}")
    private String tableId;

    @Override
    public void write(Chunk<? extends InsertAllRequest.RowToInsert> chunk) throws Exception {
        if (chunk.isEmpty()) return;

        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
        TableId tId = TableId.of(datasetId, tableId);

        InsertAllRequest.Builder reqBuilder = InsertAllRequest.newBuilder(tId);
        for (InsertAllRequest.RowToInsert row : chunk) {
            reqBuilder.addRow(row);
        }

        InsertAllResponse response = bigquery.insertAll(reqBuilder.build());
        
        if (response.hasErrors()) {
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                log.error("BigQuery insert error for row index {}: {}", entry.getKey(), entry.getValue());
            }
            throw new RuntimeException("Errors occurred while inserting to BigQuery");
        } else {
            log.info("Inserted {} rows into BigQuery table {}", chunk.size(), tableId);
        }
    }
}
