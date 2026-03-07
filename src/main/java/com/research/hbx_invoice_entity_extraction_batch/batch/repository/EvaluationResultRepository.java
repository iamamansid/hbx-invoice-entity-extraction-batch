package com.research.hbx_invoice_entity_extraction_batch.batch.repository;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.EvaluationResult;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface EvaluationResultRepository extends JpaRepository<EvaluationResult, EvaluationResult.EvaluationResultId> {
}
