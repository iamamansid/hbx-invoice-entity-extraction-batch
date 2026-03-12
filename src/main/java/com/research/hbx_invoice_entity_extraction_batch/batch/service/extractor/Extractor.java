// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/extractor/Extractor.java
package com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionRunResult;

public interface Extractor {
    ExtractionRunResult extract(String invoiceId, String text, int runNumber);
    String getModelName();

    default double getTemperature(int runNumber) {
        return switch (runNumber) {
            case 1 -> 0.0;
            case 2 -> 0.7;
            case 3 -> 1.0;
            default -> 0.0;
        };
    }
}
