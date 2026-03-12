package com.research.hbx_invoice_entity_extraction_batch.batch.step.groundtruth;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.GroundTruthConsensus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.listener.SkipListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GroundTruthConsensusSkipListener implements SkipListener<String, GroundTruthConsensus> {

    @Override
    public void onSkipInRead(Throwable t) {
        log.error("Ground truth consensus read skip", t);
    }

    @Override
    public void onSkipInWrite(GroundTruthConsensus item, Throwable t) {
        String invoiceId = item == null ? "<null>" : item.getInvoiceId();
        log.error("Ground truth consensus write skip for invoice {}", invoiceId, t);
    }

    @Override
    public void onSkipInProcess(String item, Throwable t) {
        String invoiceId = item == null ? "<null>" : item;
        log.error("Ground truth consensus process skip for invoice {}", invoiceId, t);
    }
}
