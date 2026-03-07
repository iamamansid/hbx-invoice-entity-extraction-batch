package com.research.hbx_invoice_entity_extraction_batch.batch.model.dto;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.EvaluationResult;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EvaluationBundle {
    private String invoiceId;
    private List<EvaluationResult> results;
}
