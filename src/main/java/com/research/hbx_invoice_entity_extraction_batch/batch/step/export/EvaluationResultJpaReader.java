package com.research.hbx_invoice_entity_extraction_batch.batch.step.export;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.EvaluationResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.repository.EvaluationResultRepository;
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
public class EvaluationResultJpaReader implements ItemReader<EvaluationResult> {

    private final EvaluationResultRepository evaluationResultRepository;
    
    private Iterator<EvaluationResult> iterator;
    private int currentPage = 0;
    private static final int PAGE_SIZE = 100;

    @Override
    public EvaluationResult read() throws Exception {
        if (iterator == null || !iterator.hasNext()) {
            Page<EvaluationResult> page = evaluationResultRepository.findAll(PageRequest.of(currentPage, PAGE_SIZE));
            if (!page.hasContent()) {
                return null;
            }
            iterator = page.iterator();
            currentPage++;
        }

        if (iterator.hasNext()) {
            return iterator.next();
        }

        return null; // Exhausted
    }
}
