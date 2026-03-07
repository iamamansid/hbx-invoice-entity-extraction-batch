package com.research.hbx_invoice_entity_extraction_batch.batch.step.extraction;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionBundle;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionRunResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.ExtractionRun;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.Invoice;
import com.research.hbx_invoice_entity_extraction_batch.batch.repository.ExtractionRunRepository;
import com.research.hbx_invoice_entity_extraction_batch.batch.repository.InvoiceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ExtractionRunJpaWriter implements ItemWriter<ExtractionBundle> {

    private final ExtractionRunRepository extractionRunRepository;
    private final InvoiceRepository invoiceRepository;

    @Override
    @Transactional
    public void write(Chunk<? extends ExtractionBundle> chunk) throws Exception {
        List<ExtractionRun> entities = new ArrayList<>();
        List<Invoice> invoicesToUpdate = new ArrayList<>();
        
        for (ExtractionBundle bundle : chunk) {
            String invoiceId = bundle.getInvoiceId();
            
            for (ExtractionRunResult res : bundle.getResults()) {
                ExtractionRun ent = ExtractionRun.builder()
                        .invoiceId(res.getInvoiceId())
                        .modelName(res.getModelName())
                        .runNumber(res.getRunNumber())
                        .extractedJson(res.getExtractedJson())
                        .latencyMs(res.getLatencyMs())
                        .tokenCount(res.getTokenCount())
                        .extractionStatus(res.getExtractionStatus())
                        .errorMessage(res.getErrorMessage())
                        .build();
                entities.add(ent);
            }
            
            // "Update invoice.extractionStatus = 'COMPLETED' in PostgreSQL"
            invoiceRepository.findById(invoiceId).ifPresent(inv -> {
                inv.setExtractionStatus("COMPLETED");
                invoicesToUpdate.add(inv);
            });
        }
        
        extractionRunRepository.saveAll(entities);
        invoiceRepository.saveAll(invoicesToUpdate);
        log.info("Saved {} extraction runs for {} invoices to PostgreSQL", entities.size(), invoicesToUpdate.size());
    }
}
