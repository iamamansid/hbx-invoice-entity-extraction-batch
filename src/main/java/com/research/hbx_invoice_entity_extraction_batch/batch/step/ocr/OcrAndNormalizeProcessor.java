package com.research.hbx_invoice_entity_extraction_batch.batch.step.ocr;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.GcsInvoiceItem;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.InvoiceOcrResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.NormalizationService;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.OcrService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OcrAndNormalizeProcessor implements ItemProcessor<GcsInvoiceItem, InvoiceOcrResult> {

    @Value("${gcp.project-id}")
    private String projectId;
    
    @Value("${gcp.bucket-name}")
    private String bucketName;

    private final OcrService ocrService;
    private final NormalizationService normalizationService;

    public OcrAndNormalizeProcessor(OcrService ocrService, NormalizationService normalizationService) {
        this.ocrService = ocrService;
        this.normalizationService = normalizationService;
    }

    @Override
    public InvoiceOcrResult process(GcsInvoiceItem item) throws Exception {
        log.info("Processing OCR for invoice {}", item.getInvoiceId());

        try {
            Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
            byte[] content = storage.readAllBytes(BlobId.of(bucketName, item.getFileName()));

            String rawText = ocrService.performOcr(content);
            String normalizedText = normalizationService.normalizeText(rawText);

            return InvoiceOcrResult.builder()
                    .invoiceId(item.getInvoiceId())
                    .gcsPath(item.getGcsPath())
                    .rawText(rawText)
                    .normalizedText(normalizedText)
                    .build();
                    
        } catch (Exception e) {
            log.error("OCR failure for invoice {}, skipping. Reason: {}", item.getInvoiceId(), e.getMessage());
            // Return dummy result with null rawText and normalizedText to indicate failure
            // but let writer handle the status update to "FAILED"
            return InvoiceOcrResult.builder()
                    .invoiceId(item.getInvoiceId())
                    .gcsPath(item.getGcsPath())
                    .rawText(null) // Indicator of failure
                    .normalizedText(null)
                    .build();
        }
    }
}
