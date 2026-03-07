package com.research.hbx_invoice_entity_extraction_batch.batch.step.ocr;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.InvoiceOcrResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.Invoice;
import com.research.hbx_invoice_entity_extraction_batch.batch.repository.InvoiceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class InvoiceJpaWriter implements ItemWriter<InvoiceOcrResult> {

    private final InvoiceRepository invoiceRepository;

    @Override
    public void write(Chunk<? extends InvoiceOcrResult> chunk) throws Exception {
        List<Invoice> entities = new ArrayList<>();
        
        for (InvoiceOcrResult result : chunk) {
            Optional<Invoice> existingOpt = invoiceRepository.findById(result.getInvoiceId());
            Invoice invoice = existingOpt.orElseGet(() -> Invoice.builder()
                    .invoiceId(result.getInvoiceId())
                    .gcsPath(result.getGcsPath())
                    .build());
            
            if (result.getRawText() != null) {
                invoice.setRawText(result.getRawText());
                invoice.setNormalizedText(result.getNormalizedText());
                invoice.setOcrStatus("COMPLETED");
            } else {
                invoice.setOcrStatus("FAILED");
            }
            
            entities.add(invoice);
        }
        
        invoiceRepository.saveAll(entities);
        log.info("Saved {} invoices to PostgreSQL", entities.size());
    }
}
