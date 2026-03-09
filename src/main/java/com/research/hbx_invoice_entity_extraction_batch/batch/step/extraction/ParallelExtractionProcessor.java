package com.research.hbx_invoice_entity_extraction_batch.batch.step.extraction;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionBundle;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionRunResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.InvoiceOcrResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor.Extractor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ParallelExtractionProcessor implements ItemProcessor<InvoiceOcrResult, ExtractionBundle> {

    private final List<Extractor> extractors;
    private final ExecutorService executorService;

    public ParallelExtractionProcessor(List<Extractor> extractors) {
        this.extractors = extractors;
        int threadCount = Math.max(8, extractors.size() * 3);
        this.executorService = Executors.newFixedThreadPool(threadCount);
        log.info("Configured {} extraction models: {}", extractors.size(),
                extractors.stream().map(Extractor::getModelName).collect(Collectors.joining(", ")));
    }

    @Override
    public ExtractionBundle process(InvoiceOcrResult item) throws Exception {
        log.info("Starting parallel extraction for invoice {}", item.getInvoiceId());

        String textToProcess = item.getNormalizedText() != null ? item.getNormalizedText() : item.getRawText();
        
        if (textToProcess == null || textToProcess.isEmpty()) {
            log.warn("Invoice {} has no text to extract; marking all model runs as FAILED", item.getInvoiceId());
            List<ExtractionRunResult> failed = new ArrayList<>();
            for (Extractor extractor : extractors) {
                for (int runNumber = 1; runNumber <= 3; runNumber++) {
                    failed.add(ExtractionRunResult.builder()
                            .invoiceId(item.getInvoiceId())
                            .modelName(extractor.getModelName())
                            .runNumber(runNumber)
                            .extractionStatus("FAILED")
                            .errorMessage("No OCR text available")
                            .build());
                }
            }
            return ExtractionBundle.builder()
                    .invoiceId(item.getInvoiceId())
                    .results(failed)
                    .build();
        }

        List<CompletableFuture<ExtractionRunResult>> futures = new ArrayList<>();

        for (Extractor extractor : extractors) {
            for (int runNumber = 1; runNumber <= 3; runNumber++) {
                final int currentRun = runNumber;
                
                CompletableFuture<ExtractionRunResult> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        return extractor.extract(item.getInvoiceId(), textToProcess, currentRun);
                    } catch (Exception e) {
                        log.error("Fatal error in extractor {} for invoice {}, run {}", extractor.getModelName(), item.getInvoiceId(), currentRun, e);
                        return ExtractionRunResult.builder()
                                .invoiceId(item.getInvoiceId())
                                .modelName(extractor.getModelName())
                                .runNumber(currentRun)
                                .extractionStatus("FAILED")
                                .errorMessage(e.getMessage())
                                .build();
                    }
                }, executorService);
                
                futures.add(future);
            }
        }

        // Wait for all to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Collect results
        List<ExtractionRunResult> results = futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());

        for (Extractor extractor : extractors) {
            long failedForModel = results.stream()
                    .filter(r -> extractor.getModelName().equals(r.getModelName()))
                    .filter(r -> "FAILED".equals(r.getExtractionStatus()))
                    .count();
            if (failedForModel == 3) {
                log.warn("Model {} failed all 3 runs for invoice {}", extractor.getModelName(), item.getInvoiceId());
            }
        }

        return ExtractionBundle.builder()
                .invoiceId(item.getInvoiceId())
                .results(results)
                .build();
    }

    @PreDestroy
    public void shutdownExecutor() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
        }
    }
}
