package com.research.hbx_invoice_entity_extraction_batch.batch.step.ocr;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.GcsInvoiceItem;
import com.research.hbx_invoice_entity_extraction_batch.batch.repository.InvoiceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemStream;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@Slf4j
@Component
@StepScope
@RequiredArgsConstructor
public class GcsInvoiceItemReader implements ItemReader<GcsInvoiceItem>, ItemStream, StepExecutionListener {

    private static final Set<String> SUPPORTED_EXTENSIONS = Set.of(
            "pdf", "png", "jpg", "jpeg", "tif", "tiff", "bmp", "webp", "gif"
    );

    @Value("${gcp.project-id}")
    private String projectId;
    
    @Value("${gcp.bucket-name}")
    private String bucketName;

    private final InvoiceRepository invoiceRepository;

    private List<GcsInvoiceItem> items;
    private int currentIndex = 0;
    private StepExecution stepExecution;

    @Override
    public GcsInvoiceItem read() {
        if (items != null && currentIndex < items.size()) {
            GcsInvoiceItem item = items.get(currentIndex);
            currentIndex++;
            return item;
        }
        return null; // Signals end of data
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        if (items == null) {
            items = new ArrayList<>();
            Set<String> completedOcrInvoiceIds = new HashSet<>(invoiceRepository.findInvoiceIdsWithCompletedOcr());
            int skippedCompletedCount = 0;
            int skippedNonFileCount = 0;

            Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
            Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix("invoices/"));
            
            for (Blob blob : blobs.iterateAll()) {
                if (!isProcessableFileBlob(blob)) {
                    skippedNonFileCount++;
                    continue;
                }

                String invoiceId = extractInvoiceId(blob.getName());
                if (completedOcrInvoiceIds.contains(invoiceId)) {
                    skippedCompletedCount++;
                    continue;
                }

                items.add(GcsInvoiceItem.builder()
                        .gcsPath("gs://" + bucketName + "/" + blob.getName())
                        .fileName(blob.getName())
                        .invoiceId(invoiceId)
                        .build());
            }
            log.info("Loaded {} invoices from GCS bucket {} (skipped {} already OCR-processed invoices, skipped {} non-file objects)",
                    items.size(), bucketName, skippedCompletedCount, skippedNonFileCount);
        }

        ExecutionContext jobContext = getJobExecutionContextOrFallback(executionContext);
        if (jobContext.containsKey("gcs.reader.current.index")) {
            currentIndex = jobContext.getInt("gcs.reader.current.index");
            log.info("Resuming GcsInvoiceItemReader at index {}", currentIndex);
        } else {
            currentIndex = 0;
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        executionContext.putInt("gcs.reader.current.index", currentIndex);
        ExecutionContext jobContext = getJobExecutionContextOrFallback(executionContext);
        jobContext.putInt("gcs.reader.current.index", currentIndex);
    }

    @Override
    public void close() throws ItemStreamException {
    }

    private String extractInvoiceId(String fileName) {
        String base = extractBaseName(fileName);
        int dotIdx = base.lastIndexOf('.');
        if (dotIdx > 0) {
            return base.substring(0, dotIdx);
        }
        return base;
    }

    private boolean isProcessableFileBlob(Blob blob) {
        if (blob == null) {
            return false;
        }

        String blobName = blob.getName();
        if (blobName == null || blobName.isBlank()) {
            return false;
        }

        // Directory markers can appear even without Blob#isDirectory() reporting true.
        if (blob.isDirectory() || blobName.endsWith("/")) {
            return false;
        }

        String baseName = extractBaseName(blobName);
        int dotIdx = baseName.lastIndexOf('.');
        if (dotIdx <= 0 || dotIdx == baseName.length() - 1) {
            return false;
        }

        String extension = baseName.substring(dotIdx + 1).toLowerCase(Locale.ROOT);
        return SUPPORTED_EXTENSIONS.contains(extension);
    }

    private String extractBaseName(String path) {
        int lastSlashIdx = path.lastIndexOf('/');
        return lastSlashIdx >= 0 ? path.substring(lastSlashIdx + 1) : path;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }

    private ExecutionContext getJobExecutionContextOrFallback(ExecutionContext stepContext) {
        if (stepExecution != null) {
            return stepExecution.getJobExecution().getExecutionContext();
        }
        return stepContext;
    }
}
