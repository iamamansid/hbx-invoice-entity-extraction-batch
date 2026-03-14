package com.research.hbx_invoice_entity_extraction_batch.batch.step.evaluation;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.EvaluationBundle;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.EvaluationResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.ExtractionRun;
import com.research.hbx_invoice_entity_extraction_batch.batch.repository.ExtractionRunRepository;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.EvaluationService;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.GroundTruthNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class EvaluationProcessor implements ItemProcessor<String, EvaluationBundle> {

    private final EvaluationService evaluationService;
    private final ExtractionRunRepository extractionRunRepository;
    private final ObjectMapper objectMapper;

    @Override
    public EvaluationBundle process(String invoiceId) throws Exception {
        log.info("Evaluating extractions for invoice {}", invoiceId);

        List<ExtractionRun> runs = extractionRunRepository.findByInvoiceId(invoiceId);
        
        if (runs.isEmpty()) {
            return EvaluationBundle.builder()
                    .invoiceId(invoiceId)
                    .results(List.of())
                    .build();
        }

        List<EvaluationResult> evals = new ArrayList<>();
        
        // Group by modelName
        Map<String, List<ExtractionRun>> runsByModel = runs.stream()
                .collect(Collectors.groupingBy(ExtractionRun::getModelName));
                
        for (Map.Entry<String, List<ExtractionRun>> entry : runsByModel.entrySet()) {
            String modelName = entry.getKey();
            List<ExtractionRun> modelRuns = entry.getValue();
            
            double consistencyScore = calculateConsistencyScore(modelRuns);
            
            for (ExtractionRun run : modelRuns) {
                if ("COMPLETED".equals(run.getExtractionStatus())) {
                    try {
                        EvaluationResult result = evaluationService.evaluateExtraction(invoiceId, modelName, run.getRunNumber());
                        result.setConsistencyScore(consistencyScore);
                        evals.add(result);
                    } catch (GroundTruthNotFoundException e) {
                        log.warn("GroundTruth error: {}", e.getMessage());
                        return EvaluationBundle.builder()
                                .invoiceId(invoiceId)
                                .results(List.of())
                                .build();
                    } catch (Exception e) {
                        log.error("Eval failed for invoice {} model {} run {}", invoiceId, modelName, run.getRunNumber(), e);
                    }
                }
            }
        }

        return EvaluationBundle.builder()
                .invoiceId(invoiceId)
                .results(evals)
                .build();
    }
    
    // consistencyScore (only computable across all 3 runs):
    // for each field (DATE, AMOUNT, COMPANY, INVOICE_NO), check if Set(run1) == Set(run2) == Set(run3)
    private double calculateConsistencyScore(List<ExtractionRun> runs) {
        Map<Integer, ExtractionRun> runsByNumber = runs.stream()
                .filter(r -> "COMPLETED".equals(r.getExtractionStatus()))
                .collect(Collectors.toMap(
                        ExtractionRun::getRunNumber,
                        r -> r,
                        (first, ignored) -> first
                ));
        if (!runsByNumber.keySet().containsAll(Set.of(1, 2, 3))) {
            return 0.0;
        }

        boolean dateConsistent = isConsistentAcrossRuns(runsByNumber, "DATE");
        boolean amountConsistent = isConsistentAcrossRuns(runsByNumber, "AMOUNT");
        boolean companyConsistent = isConsistentAcrossRuns(runsByNumber, "COMPANY");
        boolean invoiceNoConsistent = isConsistentAcrossRuns(runsByNumber, "INVOICE_NO");

        int consistentFields = 0;
        if (dateConsistent) consistentFields++;
        if (amountConsistent) consistentFields++;
        if (companyConsistent) consistentFields++;
        if (invoiceNoConsistent) consistentFields++;

        return consistentFields / 4.0;
    }

    private boolean isConsistentAcrossRuns(Map<Integer, ExtractionRun> runsByNumber, String field) {
        try {
            Set<String> run1 = extractSetForField(runsByNumber.get(1).getExtractedJson(), field);
            Set<String> run2 = extractSetForField(runsByNumber.get(2).getExtractedJson(), field);
            Set<String> run3 = extractSetForField(runsByNumber.get(3).getExtractedJson(), field);
            return run1.equals(run2) && run2.equals(run3);
        } catch (Exception e) {
            return false;
        }
    }
    
    private Set<String> extractSetForField(String json, String field) throws Exception {
        Set<String> set = new HashSet<>();
        if (json == null) return set;
        
        JsonNode root = objectMapper.readTree(json);
        if (root.has(field)) {
            JsonNode node = root.get(field);
            if (node.isArray()) {
                node.forEach(n -> set.add(n.asText()));
            } else {
                set.add(node.asText());
            }
        }
        return set;
    }
}
