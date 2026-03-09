// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/step/groundtruth/GroundTruthConsensusProcessor.java
package com.research.hbx_invoice_entity_extraction_batch.batch.step.groundtruth;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.ExtractionRun;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.GroundTruthConsensus;
import com.research.hbx_invoice_entity_extraction_batch.batch.repository.ExtractionRunRepository;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.ConsensusGroundTruthService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class GroundTruthConsensusProcessor implements ItemProcessor<String, GroundTruthConsensus> {

    private final ExtractionRunRepository extractionRunRepository;
    private final ConsensusGroundTruthService consensusGroundTruthService;

    @Override
    public GroundTruthConsensus process(String invoiceId) throws Exception {
        List<ExtractionRun> runs = extractionRunRepository.findByInvoiceIdAndRunNumber(invoiceId, 1);
        if (runs == null || runs.size() < 2) {
            log.warn("Skipping {} - insufficient runs", invoiceId);
            return null;
        }

        ConsensusGroundTruthService.ConsensusResult result =
                consensusGroundTruthService.computeConsensus(invoiceId, runs);

        if (result.fieldsWithConsensus() == 0) {
            log.warn("No consensus reached for invoice {}", invoiceId);
            return null;
        }

        return GroundTruthConsensus.builder()
                .invoiceId(invoiceId)
                .consensusInvoiceNo(result.invoiceNo())
                .consensusDates(joinAsCsv(result.dates()))
                .consensusAmounts(joinAsCsv(result.amounts()))
                .consensusCompanies(joinAsCsv(result.companies()))
                .modelsAgreed(result.modelsAgreed())
                .fieldsWithConsensus(result.fieldsWithConsensus())
                .seededToNeo4j(false)
                .createdAt(LocalDateTime.now())
                .build();
    }

    private String joinAsCsv(Set<String> values) {
        if (values == null || values.isEmpty()) {
            return "";
        }
        List<String> list = new ArrayList<>(values);
        Collections.sort(list);
        return String.join(",", list);
    }
}
