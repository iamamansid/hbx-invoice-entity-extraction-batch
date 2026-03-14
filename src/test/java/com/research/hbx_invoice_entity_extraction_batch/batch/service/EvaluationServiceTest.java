package com.research.hbx_invoice_entity_extraction_batch.batch.service;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.GroundTruthConsensus;
import com.research.hbx_invoice_entity_extraction_batch.batch.repository.GroundTruthConsensusRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.driver.Driver;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EvaluationServiceTest {

    @Mock
    private Driver neo4jDriver;

    @Mock
    private GroundTruthConsensusRepository groundTruthConsensusRepository;

    private EvaluationService evaluationService;

    @BeforeEach
    void setUp() {
        evaluationService = new EvaluationService(neo4jDriver, groundTruthConsensusRepository);
    }

    @Test
    void resolveGroundTruthInvoiceNoUsesConsensusInvoiceNumberWhenPresent() {
        when(groundTruthConsensusRepository.findById("anonymized_variant_8"))
                .thenReturn(Optional.of(GroundTruthConsensus.builder()
                        .invoiceId("anonymized_variant_8")
                        .consensusInvoiceNo("  GFD12345678901  ")
                        .build()));

        String resolvedInvoiceNo = evaluationService.resolveGroundTruthInvoiceNo("anonymized_variant_8");

        assertThat(resolvedInvoiceNo).isEqualTo("GFD12345678901");
    }

    @Test
    void resolveGroundTruthInvoiceNoFallsBackToInvoiceIdWhenConsensusInvoiceNumberMissing() {
        when(groundTruthConsensusRepository.findById("anonymized_variant_14"))
                .thenReturn(Optional.of(GroundTruthConsensus.builder()
                        .invoiceId("anonymized_variant_14")
                        .consensusInvoiceNo("   ")
                        .build()));

        String resolvedInvoiceNo = evaluationService.resolveGroundTruthInvoiceNo("anonymized_variant_14");

        assertThat(resolvedInvoiceNo).isEqualTo("anonymized_variant_14");
    }
}
