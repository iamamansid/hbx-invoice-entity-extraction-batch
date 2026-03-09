package com.research.hbx_invoice_entity_extraction_batch.batch.repository;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.Invoice;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface InvoiceRepository extends JpaRepository<Invoice, String> {

    @Query("SELECT i.invoiceId FROM Invoice i WHERE i.ocrStatus = 'COMPLETED'")
    List<String> findInvoiceIdsWithCompletedOcr();

    @Query("SELECT i FROM Invoice i WHERE i.ocrStatus = 'COMPLETED' AND i.extractionStatus IS NULL")
    Page<Invoice> findInvoicesForExtraction(Pageable pageable);

    @Query("SELECT i.invoiceId FROM Invoice i WHERE i.extractionStatus = 'COMPLETED' AND i.evaluationStatus IS NULL")
    Page<String> findInvoiceIdsForEvaluation(Pageable pageable);
}
