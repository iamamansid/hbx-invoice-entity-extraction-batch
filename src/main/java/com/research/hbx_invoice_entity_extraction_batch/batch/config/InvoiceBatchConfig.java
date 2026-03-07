package com.research.hbx_invoice_entity_extraction_batch.batch.config;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.EvaluationBundle;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionBundle;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.GcsInvoiceItem;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.InvoiceOcrResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.EvaluationResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.evaluation.EvaluationProcessor;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.evaluation.EvaluationResultJpaWriter;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.evaluation.PostgresExtractionRunReader;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.export.BigQueryItemWriter;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.export.BigQueryRowProcessor;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.export.EvaluationResultJpaReader;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.extraction.ExtractionRunJpaWriter;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.extraction.ExtractionRunNeo4jWriter;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.extraction.ParallelExtractionProcessor;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.extraction.PostgresInvoiceItemReader;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.ocr.GcsInvoiceItemReader;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.ocr.InvoiceJpaWriter;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.ocr.OcrAndNormalizeProcessor;
import lombok.RequiredArgsConstructor;
import com.google.cloud.bigquery.InsertAllRequest;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.support.CompositeItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Arrays;

@Configuration
@RequiredArgsConstructor
public class InvoiceBatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    // OCR Step Components
    private final GcsInvoiceItemReader ocrReader;
    private final OcrAndNormalizeProcessor ocrProcessor;
    private final InvoiceJpaWriter ocrWriter;

    // Extraction Step Components
    private final PostgresInvoiceItemReader extractionReader;
    private final ParallelExtractionProcessor extractionProcessor;
    private final ExtractionRunJpaWriter extractionJpaWriter;
    private final ExtractionRunNeo4jWriter extractionNeo4jWriter;

    // Evaluation Step Components
    private final PostgresExtractionRunReader evaluationReader;
    private final EvaluationProcessor evaluationProcessor;
    private final EvaluationResultJpaWriter evaluationWriter;

    // Export Step Components
    private final EvaluationResultJpaReader exportReader;
    private final BigQueryRowProcessor exportProcessor;
    private final BigQueryItemWriter exportWriter;

    @Bean
    public Job invoiceExtractionJob() {
        return new JobBuilder("invoiceExtractionJob", jobRepository)
                .start(ocrStep())
                .next(extractionStep())
                .next(evaluationStep())
                .next(bigQueryExportStep())
                .build();
    }

    @Bean
    public Step ocrStep() {
        return new StepBuilder("ocrStep", jobRepository)
                .<GcsInvoiceItem, InvoiceOcrResult>chunk(10, transactionManager)
                .reader(ocrReader)
                .processor(ocrProcessor)
                .writer(ocrWriter)
                .faultTolerant()
                // Retry is handled within OcrService directly using Spring Retry on the specific IOException
                // The prompt specified "Retry: @Retryable(maxAttempts=3, backoff=@Backoff(delay=1000)) on IOException"
                .build();
    }

    @Bean
    public Step extractionStep() {
        CompositeItemWriter<ExtractionBundle> compositeWriter = new CompositeItemWriter<>();
        // Neo4j runs first so failures can be reflected as NEO4J_FAILED before JPA persistence.
        compositeWriter.setDelegates(Arrays.asList(extractionNeo4jWriter, extractionJpaWriter));

        return new StepBuilder("extractionStep", jobRepository)
                .<InvoiceOcrResult, ExtractionBundle>chunk(10, transactionManager)
                .reader(extractionReader)
                .processor(extractionProcessor)
                .writer(compositeWriter)
                // LLM and Neo4j specific fault handling
                // Fault tolerance: if one model fails all 3 runs, log and continue. 
                // Handled in ParallelExtractionProcessor (returns FAILED status rather than throwing)
                // Neo4j write failure: log error, mark as NEO4J_FAILED (handled in Writer)
                .build();
    }

    @Bean
    public Step evaluationStep() {
        return new StepBuilder("evaluationStep", jobRepository)
                .<String, EvaluationBundle>chunk(10, transactionManager)
                .reader(evaluationReader)
                .processor(evaluationProcessor)
                .writer(evaluationWriter)
                .build();
    }

    @Bean
    public Step bigQueryExportStep() {
        return new StepBuilder("bigQueryExportStep", jobRepository)
                .<EvaluationResult, InsertAllRequest.RowToInsert>chunk(100, transactionManager)
                .reader(exportReader)
                .processor(exportProcessor)
                .writer(exportWriter)
                .build();
    }
}
