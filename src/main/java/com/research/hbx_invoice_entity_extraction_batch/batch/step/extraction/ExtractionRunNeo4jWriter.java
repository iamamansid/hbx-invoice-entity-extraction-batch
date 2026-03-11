package com.research.hbx_invoice_entity_extraction_batch.batch.step.extraction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionBundle;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionRunResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.Session;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class ExtractionRunNeo4jWriter implements ItemWriter<ExtractionBundle> {

    private final Driver neo4jDriver;
    private final ObjectMapper objectMapper;

    @Value("${batch.neo4j.write.max-attempts:3}")
    private int maxWriteAttempts;

    @Value("${batch.neo4j.write.backoff-ms:2000}")
    private long writeBackoffMs;

    @Override
    public void write(Chunk<? extends ExtractionBundle> chunk) throws Exception {
        int attempts = Math.max(1, maxWriteAttempts);
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try (Session session = neo4jDriver.session()) {
                writeChunk(session, chunk);
                return;
            } catch (Exception e) {
                if (!isRetryableNeo4jFailure(e) || attempt == attempts) {
                    log.error("Neo4j chunk write failed after {} attempt(s)", attempt, e);
                    markChunkAsNeo4jFailed(chunk, e);
                    return;
                }
                log.warn("Neo4j chunk write attempt {} failed due to retryable error: {}",
                        attempt, e.getMessage());
                sleepBeforeRetry(attempt);
            }
        }
    }

    private void writeChunk(Session session, Chunk<? extends ExtractionBundle> chunk) {
        for (ExtractionBundle bundle : chunk) {
            for (ExtractionRunResult res : bundle.getResults()) {
                if (!"COMPLETED".equals(res.getExtractionStatus()) || res.getExtractedJson() == null) {
                    continue;
                }

                try {
                    JsonNode extractedData = objectMapper.readTree(res.getExtractedJson());
                    writeToNeo4jWithRetry(session, res, extractedData);
                } catch (Exception e) {
                    markNeo4jFailed(res, e);
                    log.error("Failed to write to Neo4j for invoice {} model {} run {}",
                            res.getInvoiceId(), res.getModelName(), res.getRunNumber(), e);
                }
            }
        }
    }

    private void writeToNeo4jWithRetry(Session session, ExtractionRunResult res, JsonNode data) {
        int attempts = Math.max(1, maxWriteAttempts);
        for (int attempt = 1; attempt <= attempts; attempt++) {
            try {
                writeToNeo4j(session, res, data);
                return;
            } catch (Exception e) {
                if (!isRetryableNeo4jFailure(e) || attempt == attempts) {
                    throw e;
                }
                log.warn("Retrying Neo4j write for invoice {} model {} run {} (attempt {}/{}) due to: {}",
                        res.getInvoiceId(), res.getModelName(), res.getRunNumber(),
                        attempt + 1, attempts, e.getMessage());
                sleepBeforeRetry(attempt);
            }
        }
    }

    private void writeToNeo4j(Session session, ExtractionRunResult res, JsonNode data) {
        String cypher = """
                MERGE (r:ExtractionRun {invoiceNo: $invoiceNo, model: $modelName, runNo: $runNumber})
                MERGE (i:InvoiceNode {invoiceNo: $invoiceNo, model: $modelName})
                MERGE (r)-[:EXTRACTED]->(i)
                """;

        session.executeWrite(tx -> {
            Map<String, Object> params = new HashMap<>();
            params.put("invoiceNo", res.getInvoiceId());
            params.put("modelName", res.getModelName());
            params.put("runNumber", res.getRunNumber());

            tx.run(cypher, params).consume();

            // Dates
            if (data.has("DATE") && data.get("DATE").isArray()) {
                for (JsonNode dateNode : data.get("DATE")) {
                    String dateQuery = """
                            MATCH (i:InvoiceNode {invoiceNo: $invoiceNo, model: $modelName})
                            MERGE (d:DateNode {value: $dateVal, model: $modelName})
                            MERGE (i)-[:HAS_DATE]->(d)
                            """;
                    Map<String, Object> dp = new HashMap<>(params);
                    dp.put("dateVal", dateNode.asText());
                    tx.run(dateQuery, dp).consume();
                }
            }

            // Amounts
            if (data.has("AMOUNT") && data.get("AMOUNT").isArray()) {
                for (JsonNode amountNode : data.get("AMOUNT")) {
                    String amountQuery = """
                            MATCH (i:InvoiceNode {invoiceNo: $invoiceNo, model: $modelName})
                            MERGE (a:AmountNode {value: $amountVal, model: $modelName})
                            MERGE (i)-[:HAS_AMOUNT]->(a)
                            """;
                    Map<String, Object> ap = new HashMap<>(params);
                    ap.put("amountVal", amountNode.asText());
                    tx.run(amountQuery, ap).consume();
                }
            }

            // Companies
            if (data.has("COMPANY") && data.get("COMPANY").isArray()) {
                for (JsonNode companyNode : data.get("COMPANY")) {
                    String compQuery = """
                            MATCH (i:InvoiceNode {invoiceNo: $invoiceNo, model: $modelName})
                            MERGE (c:CompanyNode {name: $compName, model: $modelName})
                            MERGE (i)-[:ISSUED_TO]->(c)
                            """;
                    Map<String, Object> cp = new HashMap<>(params);
                    cp.put("compName", companyNode.asText());
                    tx.run(compQuery, cp).consume();
                }
            }

            return null;
        });
    }

    private void markChunkAsNeo4jFailed(Chunk<? extends ExtractionBundle> chunk, Exception e) {
        for (ExtractionBundle bundle : chunk) {
            for (ExtractionRunResult res : bundle.getResults()) {
                if ("COMPLETED".equals(res.getExtractionStatus()) && res.getExtractedJson() != null) {
                    markNeo4jFailed(res, e);
                }
            }
        }
    }

    private void markNeo4jFailed(ExtractionRunResult res, Exception e) {
        res.setExtractionStatus("NEO4J_FAILED");
        String neo4jMessage = "Neo4j write failed: " + e.getMessage();
        if (res.getErrorMessage() == null || res.getErrorMessage().isBlank()) {
            res.setErrorMessage(neo4jMessage);
        } else {
            res.setErrorMessage(res.getErrorMessage() + " | " + neo4jMessage);
        }
    }

    private boolean isRetryableNeo4jFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof TransientException
                    || current instanceof SessionExpiredException
                    || current instanceof ServiceUnavailableException) {
                return true;
            }
            if (current instanceof ClientException) {
                String message = current.getMessage();
                if (message != null
                        && message.toLowerCase().contains("unable to acquire connection from the pool")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private void sleepBeforeRetry(int attempt) {
        long delayMs = Math.max(200L, writeBackoffMs) * attempt;
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
