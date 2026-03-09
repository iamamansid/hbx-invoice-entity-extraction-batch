// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/GroundTruthNeo4jSeederService.java
package com.research.hbx_invoice_entity_extraction_batch.batch.service;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.GroundTruthConsensus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class GroundTruthNeo4jSeederService {

    private final Driver neo4jDriver;

    public void seedGroundTruth(GroundTruthConsensus gt) {
        try (Session session = neo4jDriver.session()) {
            session.executeWrite(tx -> {
                String invoiceNo = resolveInvoiceNo(gt);

                // Step A
                tx.run("""
                        MERGE (i:InvoiceNode {invoiceNo: $invoiceNo, isGroundTruth: true})
                        ON CREATE SET i.model = 'ground_truth', i.createdAt = datetime()
                        ON MATCH SET i.updatedAt = datetime()
                        """, Map.of("invoiceNo", invoiceNo));

                // Step B
                tx.run("""
                        MATCH (i:InvoiceNode {invoiceNo: $invoiceNo, isGroundTruth: true})
                        OPTIONAL MATCH (i)-[:HAS_DATE]->(d:DateNode {isGroundTruth: true})
                        OPTIONAL MATCH (i)-[:HAS_AMOUNT]->(a:AmountNode {isGroundTruth: true})
                        OPTIONAL MATCH (i)-[:ISSUED_TO]->(c:CompanyNode {isGroundTruth: true})
                        DETACH DELETE d, a, c
                        """, Map.of("invoiceNo", invoiceNo));

                // Step C
                for (String value : splitCsv(gt.getConsensusDates())) {
                    tx.run("""
                            MATCH (i:InvoiceNode {invoiceNo: $invoiceNo, isGroundTruth: true})
                            MERGE (d:DateNode {value: $value, isGroundTruth: true})
                            MERGE (i)-[:HAS_DATE]->(d)
                            """, Map.of("invoiceNo", invoiceNo, "value", value));
                }

                // Step D
                for (String value : splitCsv(gt.getConsensusAmounts())) {
                    tx.run("""
                            MATCH (i:InvoiceNode {invoiceNo: $invoiceNo, isGroundTruth: true})
                            MERGE (a:AmountNode {value: $value, isGroundTruth: true})
                            MERGE (i)-[:HAS_AMOUNT]->(a)
                            """, Map.of("invoiceNo", invoiceNo, "value", value));
                }

                // Step E
                for (String value : splitCsv(gt.getConsensusCompanies())) {
                    tx.run("""
                            MATCH (i:InvoiceNode {invoiceNo: $invoiceNo, isGroundTruth: true})
                            MERGE (c:CompanyNode {name: $value, isGroundTruth: true})
                            MERGE (i)-[:ISSUED_TO]->(c)
                            """, Map.of("invoiceNo", invoiceNo, "value", value));
                }
                return null;
            });
        } catch (Exception e) {
            log.error("Failed to seed GT for invoice {}", gt.getInvoiceId(), e);
            throw new RuntimeException("Failed to seed GT to Neo4j for invoice " + gt.getInvoiceId(), e);
        }
    }

    private String resolveInvoiceNo(GroundTruthConsensus gt) {
        if (gt.getConsensusInvoiceNo() != null && !gt.getConsensusInvoiceNo().isBlank()) {
            return gt.getConsensusInvoiceNo().trim();
        }
        return gt.getInvoiceId();
    }

    private List<String> splitCsv(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        return Arrays.stream(csv.split(","))
                .filter(s -> !s.isBlank())
                .map(String::trim)
                .collect(Collectors.toList());
    }
}
