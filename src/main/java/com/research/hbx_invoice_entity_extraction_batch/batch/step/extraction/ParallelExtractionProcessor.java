package com.research.hbx_invoice_entity_extraction_batch.batch.step.extraction;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionBundle;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionRunResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.InvoiceOcrResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor.Extractor;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor.NemotronExtractor;
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
import java.util.stream.Stream;

@Slf4j
@Component
public class ParallelExtractionProcessor implements ItemProcessor<InvoiceOcrResult, ExtractionBundle> {

    private final List<Extractor> extractors;
    private final NemotronExtractor nemotronExtractor;
    private final ExecutorService executorService;

    public ParallelExtractionProcessor(List<Extractor> extractors, NemotronExtractor nemotronExtractor) {
        this.nemotronExtractor = nemotronExtractor;
        this.extractors = extractors.stream()
                .filter(extractor -> extractor != nemotronExtractor)
                .toList();
        int threadCount = Math.max(8, (this.extractors.size() + 1) * 3);
        this.executorService = Executors.newFixedThreadPool(threadCount);
        log.info("Configured {} extraction models: {}", this.extractors.size() + 1,
                Stream.concat(this.extractors.stream().map(Extractor::getModelName), Stream.of(nemotronExtractor.getModelName()))
                        .collect(Collectors.joining(", ")));
    }

    @Override
    public ExtractionBundle process(InvoiceOcrResult item) throws Exception {
        log.info("Starting parallel extraction for invoice {}", item.getInvoiceId());

        String textToProcess = item.getNormalizedText() != null ? item.getNormalizedText() : item.getRawText();
        
        List<ExtractionRunResult> results = new ArrayList<>();
        if (textToProcess == null || textToProcess.isEmpty()) {
            log.warn("Invoice {} has no text to extract; marking text model runs as FAILED", item.getInvoiceId());
            for (Extractor extractor : extractors) {
                for (int runNumber = 1; runNumber <= 3; runNumber++) {
                    results.add(ExtractionRunResult.builder()
                            .invoiceId(item.getInvoiceId())
                            .modelName(extractor.getModelName())
                            .runNumber(runNumber)
                            .extractionStatus("FAILED")
                            .errorMessage("No OCR text available")
                            .build());
                }
            }
        }

        List<CompletableFuture<ExtractionRunResult>> futures = new ArrayList<>();

        if (textToProcess != null && !textToProcess.isEmpty()) {
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
        }

        CompletableFuture<List<ExtractionRunResult>> nemotronFuture = CompletableFuture.supplyAsync(() -> {
            List<ExtractionRunResult> nemotronResults = new ArrayList<>();
            for (int run = 1; run <= 3; run++) {
                try {
                    nemotronResults.add(nemotronExtractor.extract(item.getInvoiceId(), item.getGcsPath(), run));
                } catch (Exception e) {
                    log.error("Fatal error in extractor {} for invoice {}, run {}", nemotronExtractor.getModelName(), item.getInvoiceId(), run, e);
                    nemotronResults.add(ExtractionRunResult.builder()
                            .invoiceId(item.getInvoiceId())
                            .modelName(nemotronExtractor.getModelName())
                            .runNumber(run)
                            .extractionStatus("FAILED")
                            .errorMessage(e.getMessage())
                            .build());
                }
            }
            return nemotronResults;
        }, executorService);

        List<CompletableFuture<?>> allFutures = new ArrayList<>(futures);
        allFutures.add(nemotronFuture);
        CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).join();

        results.addAll(futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList()));
        results.addAll(nemotronFuture.join());

        List<Extractor> allExtractors = Stream.concat(extractors.stream(), Stream.of(nemotronExtractor)).toList();
        for (Extractor extractor : allExtractors) {
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
