// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/step/groundtruth/GroundTruthConsensusWriter.java
package com.research.hbx_invoice_entity_extraction_batch.batch.step.groundtruth;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.GroundTruthConsensus;
import com.research.hbx_invoice_entity_extraction_batch.batch.repository.GroundTruthConsensusRepository;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.GroundTruthNeo4jSeederService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class GroundTruthConsensusWriter implements ItemWriter<GroundTruthConsensus> {

    private final GroundTruthConsensusRepository groundTruthConsensusRepository;
    private final GroundTruthNeo4jSeederService groundTruthNeo4jSeederService;

    @Override
    public void write(Chunk<? extends GroundTruthConsensus> chunk) throws Exception {
        if (chunk == null || chunk.isEmpty()) {
            log.info("GroundTruthConsensus writer received an empty chunk");
            return;
        }
        log.info("Writing {} ground truth consensus rows", chunk.size());

        for (GroundTruthConsensus item : chunk) {
            log.info(
                    "Persisting consensus for invoice {} (modelsAgreed={}, fieldsWithConsensus={})",
                    item.getInvoiceId(),
                    item.getModelsAgreed(),
                    item.getFieldsWithConsensus()
            );
            groundTruthConsensusRepository.save(item); // PostgreSQL first
            try {
                groundTruthNeo4jSeederService.seedGroundTruth(item);
                item.setSeededToNeo4j(true);
                groundTruthConsensusRepository.save(item);
                log.info("Seeded ground truth to Neo4j for invoice {}", item.getInvoiceId());
            } catch (Exception e) {
                log.error("Neo4j seeding failed for {}", item.getInvoiceId(), e);
                // Do not rethrow: PostgreSQL record exists and remains pending for retry.
            }
        }
    }
}
