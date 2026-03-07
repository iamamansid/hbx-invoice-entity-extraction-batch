package com.research.hbx_invoice_entity_extraction_batch.batch.step.extraction;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.InvoiceOcrResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.Invoice;
import com.research.hbx_invoice_entity_extraction_batch.batch.repository.InvoiceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Component;

import java.util.Iterator;

@Slf4j
@Component
@StepScope
@RequiredArgsConstructor
public class PostgresInvoiceItemReader implements ItemReader<InvoiceOcrResult> {

    private final InvoiceRepository invoiceRepository;
    
    // Spring Batch item readers are often step-scoped and stateful.
    private Iterator<Invoice> invoiceIterator;
    private int currentPage = 0;
    private static final int PAGE_SIZE = 100;

    @Override
    public InvoiceOcrResult read() throws Exception {
        if (invoiceIterator == null || !invoiceIterator.hasNext()) {
            Page<Invoice> page = invoiceRepository.findInvoicesForExtraction(PageRequest.of(currentPage, PAGE_SIZE));
            if (!page.hasContent()) {
                return null;
            }
            invoiceIterator = page.iterator();
            currentPage++; // Advance to next page for the next trigger
        }

        if (invoiceIterator.hasNext()) {
            Invoice inv = invoiceIterator.next();
            log.info("Reading invoice {} for extraction", inv.getInvoiceId());
            return InvoiceOcrResult.builder()
                    .invoiceId(inv.getInvoiceId())
                    .gcsPath(inv.getGcsPath())
                    .rawText(inv.getRawText())
                    .normalizedText(inv.getNormalizedText())
                    .build();
        }

        return null; // Exhausted
    }
}
