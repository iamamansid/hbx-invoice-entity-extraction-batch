package com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionRunResult;

public interface Extractor {
    ExtractionRunResult extract(String invoiceId, String text, int runNumber);
    String getModelName();
}
