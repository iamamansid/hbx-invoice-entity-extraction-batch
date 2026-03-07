package com.research.hbx_invoice_entity_extraction_batch.batch.step.export;

import com.google.cloud.bigquery.InsertAllRequest;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.EvaluationResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class BigQueryRowProcessor implements ItemProcessor<EvaluationResult, InsertAllRequest.RowToInsert> {

    @Override
    public InsertAllRequest.RowToInsert process(EvaluationResult result) throws Exception {
        Map<String, Object> rowContent = new HashMap<>();
        rowContent.put("invoice_id", result.getInvoiceId());
        rowContent.put("model_name", result.getModelName());
        rowContent.put("run_number", result.getRunNumber());
        rowContent.put("date_accuracy", result.getDateAccuracy());
        rowContent.put("date_f1", result.getDateF1());
        rowContent.put("amount_accuracy", result.getAmountAccuracy());
        rowContent.put("amount_f1", result.getAmountF1());
        rowContent.put("company_accuracy", result.getCompanyAccuracy());
        rowContent.put("company_f1", result.getCompanyF1());
        rowContent.put("overall_entity_accuracy", result.getOverallEntityAccuracy());
        rowContent.put("relation_completeness", result.getRelationCompleteness());
        rowContent.put("query_answerability", result.getQueryAnswerability());
        rowContent.put("graph_idempotent", result.getGraphIdempotent());
        rowContent.put("consistency_score", result.getConsistencyScore());
        rowContent.put("created_at", result.getCreatedAt() != null ? result.getCreatedAt().toString() : null);

        // using invoice_id + modelName + runNumber as insert ID to deduplicate in BQ if needed
        String insertId = String.format("%s_%s_%d", result.getInvoiceId(), result.getModelName(), result.getRunNumber());
        
        return InsertAllRequest.RowToInsert.of(insertId, rowContent);
    }
}
