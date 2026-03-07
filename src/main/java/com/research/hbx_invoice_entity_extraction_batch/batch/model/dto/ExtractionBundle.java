package com.research.hbx_invoice_entity_extraction_batch.batch.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExtractionBundle {
    private String invoiceId;
    private List<ExtractionRunResult> results;
}
