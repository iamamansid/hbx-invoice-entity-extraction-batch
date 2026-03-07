package com.research.hbx_invoice_entity_extraction_batch.batch.step.evaluation;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.EvaluationBundle;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.EvaluationResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.Invoice;
import com.research.hbx_invoice_entity_extraction_batch.batch.repository.EvaluationResultRepository;
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
public class EvaluationResultJpaWriter implements ItemWriter<EvaluationBundle> {

    private final EvaluationResultRepository evaluationResultRepository;
    private final InvoiceRepository invoiceRepository;

    @Override
    @Transactional
    public void write(Chunk<? extends EvaluationBundle> chunk) throws Exception {
        List<EvaluationResult> allResults = new ArrayList<>();
        List<Invoice> invoicesToUpdate = new ArrayList<>();
        
        for (EvaluationBundle bundle : chunk) {
            String invoiceId = bundle.getInvoiceId();
            
            if (bundle.getResults() != null && !bundle.getResults().isEmpty()) {
                for (EvaluationResult r : bundle.getResults()) {
                    boolean isValid = r.getModelName() != null && r.getRunNumber() != null;
                    if (isValid) {
                        allResults.add(r);
                    }
                }
                
                invoiceRepository.findById(invoiceId).ifPresent(inv -> {
                    inv.setEvaluationStatus("COMPLETED");
                    invoicesToUpdate.add(inv);
                });
            } else {
                invoiceRepository.findById(invoiceId).ifPresent(inv -> {
                    inv.setEvaluationStatus("SKIPPED");
                    invoicesToUpdate.add(inv);
                });
            }
        }
        
        evaluationResultRepository.saveAll(allResults);
        invoiceRepository.saveAll(invoicesToUpdate);
        log.info("Saved {} evaluation results for {} invoices to PostgreSQL", allResults.size(), invoicesToUpdate.size());
    }
}
