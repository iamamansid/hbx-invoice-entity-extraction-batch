package com.research.hbx_invoice_entity_extraction_batch.batch.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExtractionRunResult {
    private String invoiceId;
    private String modelName;
    private Integer runNumber;
    private String extractedJson;
    private Integer latencyMs;
    private Integer tokenCount;
    private String extractionStatus;
    private String errorMessage;
}
