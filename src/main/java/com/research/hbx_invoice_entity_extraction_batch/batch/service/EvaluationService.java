package com.research.hbx_invoice_entity_extraction_batch.batch.service;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.EvaluationResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.repository.GroundTruthConsensusRepository;
import lombok.RequiredArgsConstructor;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class EvaluationService {

    private final Driver neo4jDriver;
    private final GroundTruthConsensusRepository groundTruthConsensusRepository;

    public EvaluationResult evaluateExtraction(String invoiceId, String modelName, int runNumber) {
        try (Session session = neo4jDriver.session()) {
            String groundTruthInvoiceNo = resolveGroundTruthInvoiceNo(invoiceId);

            // 1. Get Ground Truth
            String gtQuery = """
                MATCH (i:InvoiceNode {isGroundTruth: true, model: 'ground_truth', invoiceNo: $invoiceNo})
                OPTIONAL MATCH (i)-[:HAS_DATE]->(d:DateNode)
                OPTIONAL MATCH (i)-[:HAS_AMOUNT]->(a:AmountNode)  
                OPTIONAL MATCH (i)-[:ISSUED_TO]->(c:CompanyNode)
                RETURN collect(DISTINCT d.value) AS dates, 
                       collect(DISTINCT a.value) AS amounts, 
                       collect(DISTINCT c.name) AS companies
                """;

            Result gtResult = session.run(gtQuery, Map.of("invoiceNo", groundTruthInvoiceNo));
            if (!gtResult.hasNext()) {
                throw new GroundTruthNotFoundException("Ground truth not found for invoice " + invoiceId);
            }

            Record gtRecord = gtResult.single();
            Set<String> gtDates = collectValues(gtRecord, "dates");
            Set<String> gtAmounts = collectValues(gtRecord, "amounts");
            Set<String> gtCompanies = collectValues(gtRecord, "companies");

            if (gtDates.isEmpty() && gtAmounts.isEmpty() && gtCompanies.isEmpty()) {
                throw new GroundTruthNotFoundException("Ground truth is empty for invoice " + invoiceId);
            }

            // 2. Get Extracted Values for this model and run
            String extractedQuery = """
                MATCH (r:ExtractionRun {invoiceNo: $invoiceNo, model: $model, runNo: $runNo})-[:EXTRACTED]->(i:InvoiceNode)
                OPTIONAL MATCH (i)-[:HAS_DATE]->(d:DateNode)
                OPTIONAL MATCH (i)-[:HAS_AMOUNT]->(a:AmountNode)  
                OPTIONAL MATCH (i)-[:ISSUED_TO]->(c:CompanyNode)
                RETURN collect(DISTINCT d.value) AS dates, 
                       collect(DISTINCT a.value) AS amounts, 
                       collect(DISTINCT c.name) AS companies
                """;

            Result exResult = session.run(extractedQuery, Map.of(
                    "invoiceNo", invoiceId,
                    "model", modelName,
                    "runNo", runNumber
            ));

            Set<String> exDates = new LinkedHashSet<>();
            Set<String> exAmounts = new LinkedHashSet<>();
            Set<String> exCompanies = new LinkedHashSet<>();

            if (exResult.hasNext()) {
                Record exRecord = exResult.single();
                exDates.addAll(collectValues(exRecord, "dates"));
                exAmounts.addAll(collectValues(exRecord, "amounts"));
                exCompanies.addAll(collectValues(exRecord, "companies"));
            }

            // Metrics
            double dateAcc = calculateAccuracy(gtDates, exDates);
            double dateF1 = calculateF1(gtDates, exDates);

            double amtAcc = calculateAccuracy(gtAmounts, exAmounts);
            double amtF1 = calculateF1(gtAmounts, exAmounts);

            double compAcc = calculateAccuracy(gtCompanies, exCompanies);
            double compF1 = calculateF1(gtCompanies, exCompanies);

            double overallAcc = (dateAcc + amtAcc + compAcc) / 3.0;

            // Relation completeness
            int expectedRels = gtDates.size() + gtAmounts.size() + gtCompanies.size();
            int extractedRels = exDates.size() + exAmounts.size() + exCompanies.size();
            double relationCompleteness = expectedRels > 0 ? Math.min(1.0, (double) extractedRels / expectedRels) : 1.0;

            // Query answerability
            double queryAns = calculateQueryAnswerability(session, invoiceId, modelName, runNumber);

            // Graph idempotency
            boolean graphIdempotent = checkGraphIdempotency(session, invoiceId, modelName, runNumber, exDates, exAmounts, exCompanies);

            return EvaluationResult.builder()
                    .invoiceId(invoiceId)
                    .modelName(modelName)
                    .runNumber(runNumber)
                    .dateAccuracy(dateAcc)
                    .dateF1(dateF1)
                    .amountAccuracy(amtAcc)
                    .amountF1(amtF1)
                    .companyAccuracy(compAcc)
                    .companyF1(compF1)
                    .overallEntityAccuracy(overallAcc)
                    .relationCompleteness(relationCompleteness)
                    .queryAnswerability(queryAns)
                    .graphIdempotent(graphIdempotent)
                    .consistencyScore(0.0)
                    .createdAt(LocalDateTime.now())
                    .build();

        }
    }

    String resolveGroundTruthInvoiceNo(String invoiceId) {
        return groundTruthConsensusRepository.findById(invoiceId)
                .map(groundTruth -> {
                    String consensusInvoiceNo = groundTruth.getConsensusInvoiceNo();
                    if (consensusInvoiceNo == null || consensusInvoiceNo.isBlank()) {
                        return invoiceId;
                    }
                    return consensusInvoiceNo.trim();
                })
                .orElse(invoiceId);
    }

    private Set<String> collectValues(Record record, String key) {
        Set<String> values = new LinkedHashSet<>();
        record.get(key).values().forEach(value -> {
            if (!value.isNull()) {
                String text = value.asString("").trim();
                if (!text.isEmpty()) {
                    values.add(text);
                }
            }
        });
        return values;
    }

    private double calculateAccuracy(Set<String> gt, Set<String> extracted) {
        if (gt.isEmpty()) return extracted.isEmpty() ? 1.0 : 0.0;
        int correct = 0;
        for (String e : extracted) {
            if (gt.contains(e)) correct++;
        }
        return (double) correct / gt.size();
    }
    
    private double calculateF1(Set<String> gt, Set<String> extracted) {
        if (gt.isEmpty() && extracted.isEmpty()) return 1.0;
        if (gt.isEmpty() || extracted.isEmpty()) return 0.0;

        int correct = 0;
        for (String e : extracted) {
            if (gt.contains(e)) correct++;
        }

        double precision = (double) correct / extracted.size();
        double recall = (double) correct / gt.size();

        if (precision + recall == 0) return 0.0;
        return 2 * precision * recall / (precision + recall);
    }

    private double calculateQueryAnswerability(Session session, String id, String model, int run) {
        int success = 0;
        Map<String, Object> params = Map.of("inv", id, "m", model, "r", run);

        String q1 = "MATCH (r:ExtractionRun {invoiceNo:$inv, model:$m, runNo:$r})-[:EXTRACTED]->(i)-[:ISSUED_TO]->(c:CompanyNode) RETURN c";
        if (session.run(q1, params).hasNext()) success++;

        String q2 = "MATCH (r:ExtractionRun {invoiceNo:$inv, model:$m, runNo:$r})-[:EXTRACTED]->(i)-[:HAS_AMOUNT]->(a:AmountNode) RETURN a";
        if (session.run(q2, params).hasNext()) success++;

        String q3 = "MATCH (r:ExtractionRun {invoiceNo:$inv, model:$m, runNo:$r})-[:EXTRACTED]->(i) RETURN i";
        if (session.run(q3, params).hasNext()) success++;

        return success / 3.0;
    }

    private boolean checkGraphIdempotency(
            Session session,
            String id,
            String model,
            int run,
            Set<String> dates,
            Set<String> amounts,
            Set<String> companies) {
        String q = "MATCH (r:ExtractionRun {invoiceNo:$inv, model:$m, runNo:$r})-[*0..2]-(n) RETURN count(DISTINCT n) AS cnt";
        int countBefore = session.run(q, Map.of("inv", id, "m", model, "r", run)).single().get("cnt").asInt(0);

        createInvoiceGraph(session, id, model, run, dates, amounts, companies);

        int countAfter = session.run(q, Map.of("inv", id, "m", model, "r", run)).single().get("cnt").asInt(0);
        return countBefore == countAfter;
    }

    private void createInvoiceGraph(
            Session session,
            String invoiceId,
            String modelName,
            int runNumber,
            Set<String> dates,
            Set<String> amounts,
            Set<String> companies) {
        try (Transaction tx = session.beginTransaction()) {
            Map<String, Object> base = Map.of(
                    "invoiceNo", invoiceId,
                    "modelName", modelName,
                    "runNumber", runNumber
            );
            tx.run("""
                    MERGE (r:ExtractionRun {invoiceNo: $invoiceNo, model: $modelName, runNo: $runNumber})
                    MERGE (i:InvoiceNode {invoiceNo: $invoiceNo, model: $modelName})
                    MERGE (r)-[:EXTRACTED]->(i)
                    """, base);

            for (String date : dates) {
                tx.run("""
                        MATCH (i:InvoiceNode {invoiceNo: $invoiceNo, model: $modelName})
                        MERGE (d:DateNode {value: $value, model: $modelName})
                        MERGE (i)-[:HAS_DATE]->(d)
                        """, Map.of("invoiceNo", invoiceId, "modelName", modelName, "value", date));
            }

            for (String amount : amounts) {
                tx.run("""
                        MATCH (i:InvoiceNode {invoiceNo: $invoiceNo, model: $modelName})
                        MERGE (a:AmountNode {value: $value, model: $modelName})
                        MERGE (i)-[:HAS_AMOUNT]->(a)
                        """, Map.of("invoiceNo", invoiceId, "modelName", modelName, "value", amount));
            }

            for (String company : companies) {
                tx.run("""
                        MATCH (i:InvoiceNode {invoiceNo: $invoiceNo, model: $modelName})
                        MERGE (c:CompanyNode {name: $value, model: $modelName})
                        MERGE (i)-[:ISSUED_TO]->(c)
                        """, Map.of("invoiceNo", invoiceId, "modelName", modelName, "value", company));
            }

            tx.commit();
        }
    }
}
