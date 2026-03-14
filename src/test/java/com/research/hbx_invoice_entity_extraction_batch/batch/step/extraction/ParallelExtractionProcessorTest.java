package com.research.hbx_invoice_entity_extraction_batch.batch.step.extraction;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionBundle;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionRunResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.InvoiceOcrResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor.Extractor;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor.NemotronExtractor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ParallelExtractionProcessorTest {

    private ParallelExtractionProcessor processor;

    @AfterEach
    void tearDown() {
        if (processor != null) {
            processor.shutdownExecutor();
        }
    }

    @Test
    void processPassesNormalizedTextToTextExtractorsAndGcsPathToNemotron() throws Exception {
        Extractor textExtractor = mock(Extractor.class);
        NemotronExtractor nemotronExtractor = mock(NemotronExtractor.class);
        when(textExtractor.getModelName()).thenReturn("text-model");
        when(nemotronExtractor.getModelName()).thenReturn("nemotron-ocr-v1");
        for (int run = 1; run <= 3; run++) {
            when(textExtractor.extract("INV-001", "normalized text", run))
                    .thenReturn(result("INV-001", "text-model", run, "COMPLETED"));
            when(nemotronExtractor.extract("INV-001", "gs://bucket/invoices/sample.png", run))
                    .thenReturn(result("INV-001", "nemotron-ocr-v1", run, "COMPLETED"));
        }

        processor = new ParallelExtractionProcessor(List.of(textExtractor, nemotronExtractor), nemotronExtractor);

        ExtractionBundle bundle = processor.process(InvoiceOcrResult.builder()
                .invoiceId("INV-001")
                .gcsPath("gs://bucket/invoices/sample.png")
                .normalizedText("normalized text")
                .rawText("raw text")
                .build());

        assertThat(bundle.getResults()).hasSize(6);
        verify(textExtractor).extract("INV-001", "normalized text", 1);
        verify(textExtractor).extract("INV-001", "normalized text", 2);
        verify(textExtractor).extract("INV-001", "normalized text", 3);
        verify(nemotronExtractor).extract("INV-001", "gs://bucket/invoices/sample.png", 1);
        verify(nemotronExtractor).extract("INV-001", "gs://bucket/invoices/sample.png", 2);
        verify(nemotronExtractor).extract("INV-001", "gs://bucket/invoices/sample.png", 3);
    }

    @Test
    void processStillRunsNemotronWhenNoTextIsAvailable() throws Exception {
        Extractor textExtractor = mock(Extractor.class);
        NemotronExtractor nemotronExtractor = mock(NemotronExtractor.class);
        when(textExtractor.getModelName()).thenReturn("text-model");
        when(nemotronExtractor.getModelName()).thenReturn("nemotron-ocr-v1");
        for (int run = 1; run <= 3; run++) {
            when(nemotronExtractor.extract("INV-002", "gs://bucket/invoices/sample.png", run))
                    .thenReturn(result("INV-002", "nemotron-ocr-v1", run, "COMPLETED"));
        }

        processor = new ParallelExtractionProcessor(List.of(textExtractor, nemotronExtractor), nemotronExtractor);

        ExtractionBundle bundle = processor.process(InvoiceOcrResult.builder()
                .invoiceId("INV-002")
                .gcsPath("gs://bucket/invoices/sample.png")
                .normalizedText(null)
                .rawText(null)
                .build());

        assertThat(bundle.getResults()).hasSize(6);
        assertThat(bundle.getResults())
                .filteredOn(result -> "text-model".equals(result.getModelName()))
                .allMatch(result -> "FAILED".equals(result.getExtractionStatus()));
        verify(textExtractor, never()).extract("INV-002", "gs://bucket/invoices/sample.png", 1);
        verify(nemotronExtractor).extract("INV-002", "gs://bucket/invoices/sample.png", 1);
        verify(nemotronExtractor).extract("INV-002", "gs://bucket/invoices/sample.png", 2);
        verify(nemotronExtractor).extract("INV-002", "gs://bucket/invoices/sample.png", 3);
    }

    private ExtractionRunResult result(String invoiceId, String modelName, int runNumber, String status) {
        return ExtractionRunResult.builder()
                .invoiceId(invoiceId)
                .modelName(modelName)
                .runNumber(runNumber)
                .extractionStatus(status)
                .build();
    }
}
