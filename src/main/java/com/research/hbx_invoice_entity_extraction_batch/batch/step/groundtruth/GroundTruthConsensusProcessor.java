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
        try {
            log.info("Processing ground truth consensus for invoice {}", invoiceId);
            List<ExtractionRun> runs = extractionRunRepository.findByInvoiceIdAndRunNumber(invoiceId, 1);
            if (runs == null || runs.size() < 2) {
                int found = runs == null ? 0 : runs.size();
                log.warn("Skipping {} - insufficient run_number=1 rows. Found={}", invoiceId, found);
                return null;
            }

            ConsensusGroundTruthService.ConsensusResult result =
                    consensusGroundTruthService.computeConsensus(invoiceId, runs);

            if (result.modelsAgreed() < 2) {
                log.warn(
                        "Skipping {} - only {} completed model outputs available for consensus",
                        invoiceId,
                        result.modelsAgreed()
                );
                return null;
            }

            log.info(
                    "Consensus evaluation for {}: modelsConsidered={}, requiredVotes={}, fieldsWithConsensus={}, invoiceNoConsensus={}, dates={}, amounts={}, companies={}",
                    invoiceId,
                    result.modelsAgreed(),
                    result.requiredVotes(),
                    result.fieldsWithConsensus(),
                    result.invoiceNo() != null,
                    result.dates().size(),
                    result.amounts().size(),
                    result.companies().size()
            );

            if (result.fieldsWithConsensus() == 0) {
                log.warn(
                        "No consensus reached for invoice {} (requiredVotes={}, modelsConsidered={})",
                        invoiceId,
                        result.requiredVotes(),
                        result.modelsAgreed()
                );
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
        } catch (Exception e) {
            log.error("Ground truth consensus processor failed for invoice {}", invoiceId, e);
            throw e;
        }
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
