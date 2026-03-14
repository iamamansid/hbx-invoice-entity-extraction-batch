// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/GcsStorageService.java
package com.research.hbx_invoice_entity_extraction_batch.batch.service;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;

@Service
@Slf4j
public class GcsStorageService {

    @Value("${gcp.project-id}")
    private String projectId;

    public byte[] downloadBytes(String gcsPath) {
        if (gcsPath == null || gcsPath.isBlank()) {
            return null;
        }

        try {
            URI uri = URI.create(gcsPath);
            if (!"gs".equalsIgnoreCase(uri.getScheme()) || uri.getHost() == null || uri.getPath() == null) {
                log.error("Invalid GCS path: {}", gcsPath);
                return null;
            }

            String objectName = uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
            Storage storage = StorageOptions.newBuilder()
                    .setProjectId(projectId)
                    .build()
                    .getService();
            return storage.readAllBytes(BlobId.of(uri.getHost(), objectName));
        } catch (Exception e) {
            log.error("Failed to download bytes from GCS path {}", gcsPath, e);
            return null;
        }
    }
}
