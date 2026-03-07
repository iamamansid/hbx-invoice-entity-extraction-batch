package com.research.hbx_invoice_entity_extraction_batch.batch.repository;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.ExtractionRun;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ExtractionRunRepository extends JpaRepository<ExtractionRun, ExtractionRun.ExtractionRunId> {
    
    @Query("SELECT DISTINCT e.invoiceId FROM ExtractionRun e WHERE e.extractionStatus = 'COMPLETED' " +
           "AND NOT EXISTS (SELECT 1 FROM EvaluationResult eval WHERE eval.invoiceId = e.invoiceId)")
    Page<String> findDistinctInvoiceIdsForEvaluation(Pageable pageable);

    List<ExtractionRun> findByInvoiceId(String invoiceId);
}
