package com.research.hbx_invoice_entity_extraction_batch.batch.step.extraction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionBundle;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionRunResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
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

    @Override
    public void write(Chunk<? extends ExtractionBundle> chunk) throws Exception {
        try (Session session = neo4jDriver.session()) {
            for (ExtractionBundle bundle : chunk) {
                for (ExtractionRunResult res : bundle.getResults()) {
                    if (!"COMPLETED".equals(res.getExtractionStatus()) || res.getExtractedJson() == null) {
                        continue;
                    }

                    try {
                        JsonNode extractedData = objectMapper.readTree(res.getExtractedJson());
                        writeToNeo4j(session, res, extractedData);
                    } catch (Exception e) {
                        res.setExtractionStatus("NEO4J_FAILED");
                        String neo4jMessage = "Neo4j write failed: " + e.getMessage();
                        if (res.getErrorMessage() == null || res.getErrorMessage().isBlank()) {
                            res.setErrorMessage(neo4jMessage);
                        } else {
                            res.setErrorMessage(res.getErrorMessage() + " | " + neo4jMessage);
                        }
                        log.error("Failed to write to Neo4j for invoice {} model {} run {}", 
                                res.getInvoiceId(), res.getModelName(), res.getRunNumber(), e);
                    }
                }
            }
        }
    }

    private void writeToNeo4j(Session session, ExtractionRunResult res, JsonNode data) {
        String cypher = """
                MERGE (r:ExtractionRun {invoiceNo: $invoiceNo, model: $modelName, runNo: $runNumber})
                MERGE (i:InvoiceNode {invoiceNo: $invoiceNo, model: $modelName})
                MERGE (r)-[:EXTRACTED]->(i)
                """;
        
        try (Transaction tx = session.beginTransaction()) {
            Map<String, Object> params = new HashMap<>();
            params.put("invoiceNo", res.getInvoiceId());
            params.put("modelName", res.getModelName());
            params.put("runNumber", res.getRunNumber());
            
            tx.run(cypher, params);
            
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
                    tx.run(dateQuery, dp);
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
                    tx.run(amountQuery, ap);
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
                    tx.run(compQuery, cp);
                }
            }
            
            tx.commit();
        }
    }
}
