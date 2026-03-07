package com.research.hbx_invoice_entity_extraction_batch.batch.step.evaluation;

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
public class PostgresExtractionRunReader implements ItemReader<String> {

    private final InvoiceRepository invoiceRepository;
    
    private Iterator<String> invoiceIdIterator;
    private int currentPage = 0;
    private static final int PAGE_SIZE = 100;

    @Override
    public String read() throws Exception {
        if (invoiceIdIterator == null || !invoiceIdIterator.hasNext()) {
            Page<String> page = invoiceRepository.findInvoiceIdsForEvaluation(PageRequest.of(currentPage, PAGE_SIZE));
            if (!page.hasContent()) {
                return null;
            }
            invoiceIdIterator = page.iterator();
            currentPage++;
        }

        if (invoiceIdIterator.hasNext()) {
            String invId = invoiceIdIterator.next();
            log.info("Reading distinct invoiceId {} for evaluation", invId);
            return invId;
        }

        return null; // Exhausted
    }
}
