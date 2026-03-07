package com.research.hbx_invoice_entity_extraction_batch.batch.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GcsInvoiceItem {
    private String gcsPath;
    private String fileName;
    private String invoiceId;
}
