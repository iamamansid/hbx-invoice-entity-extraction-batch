// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/extractor/NemotronExtractor.java
package com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionRunResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.nemotron.NemotronOcrResponse;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.GcsStorageService;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.NormalizationService;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.nemotron.NemotronEntityParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
@Slf4j
@RequiredArgsConstructor
public class NemotronExtractor implements Extractor {

    private static final String MODEL_NAME = "nemotron-ocr-v1";

    private final NemotronEntityParser entityParser;
    private final NormalizationService normalizationService;
    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate;
    private final GcsStorageService gcsStorageService;

    @Value("${nvidia.api.key}")
    private String apiKey;

    @Value("${nvidia.api.endpoint}")
    private String apiEndpoint;

    @Override
    public ExtractionRunResult extract(String invoiceId, String gcsPath, int runNumber) {
        long start = System.currentTimeMillis();

        try {
            byte[] imageBytes = gcsStorageService.downloadBytes(gcsPath);
            if (imageBytes == null || imageBytes.length == 0) {
                return failedResult(invoiceId, runNumber, start, "Failed to download image");
            }

            String mediaType = gcsPath != null && gcsPath.toLowerCase(Locale.ROOT).endsWith(".png")
                    ? "image/png"
                    : "image/jpeg";
            String base64 = Base64.getEncoder().encodeToString(imageBytes);
            String dataUrl = "data:" + mediaType + ";base64," + base64;

            JsonNode responseBody = callNemotronApi(dataUrl, invoiceId);
            if (responseBody == null) {
                return failedResult(invoiceId, runNumber, start, "Nemotron API call failed");
            }

            NemotronOcrResponse ocrResponse = NemotronOcrResponse.from(responseBody);
            if (ocrResponse.getDetections().isEmpty()) {
                log.warn("Nemotron returned empty detections for {}", invoiceId);
                return failedResult(invoiceId, runNumber, start, "Empty detections");
            }

            NemotronEntityParser.ParsedEntities entities = entityParser.parse(ocrResponse);

            ObjectNode root = objectMapper.createObjectNode();
            if (entities.invoiceNo() == null) {
                root.putNull("INVOICE_NO");
            } else {
                root.put("INVOICE_NO", entities.invoiceNo());
            }
            ArrayNode dateArray = root.putArray("DATE");
            entities.dates().forEach(dateArray::add);
            ArrayNode amountArray = root.putArray("AMOUNT");
            entities.amounts().forEach(amountArray::add);
            ArrayNode companyArray = root.putArray("COMPANY");
            entities.companies().forEach(companyArray::add);

            return ExtractionRunResult.builder()
                    .invoiceId(invoiceId)
                    .modelName(getModelName())
                    .runNumber(runNumber)
                    .extractedJson(objectMapper.writeValueAsString(root))
                    .latencyMs((int) (System.currentTimeMillis() - start))
                    .tokenCount(null)
                    .extractionStatus("COMPLETED")
                    .build();
        } catch (Exception e) {
            log.error("Nemotron extraction failed for invoice {}", invoiceId, e);
            return failedResult(invoiceId, runNumber, start, e.getMessage());
        }
    }

    @Override
    public String getModelName() {
        return MODEL_NAME;
    }

    @Override
    public double getTemperature(int runNumber) {
        return 0.0;
    }

    @Retryable(
            retryFor = {RestClientException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 2000, multiplier = 2)
    )
    private JsonNode callNemotronApi(String dataUrl, String invoiceId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));
        headers.setBearerAuth(apiKey);

        Map<String, Object> requestBody = Map.of(
                "input", List.of(Map.of(
                        "type", "image_url",
                        "url", dataUrl
                ))
        );

        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(requestBody, headers);
        long delayMs = 2000L;

        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                ResponseEntity<JsonNode> response = restTemplate.exchange(
                        apiEndpoint,
                        HttpMethod.POST,
                        entity,
                        JsonNode.class
                );
                if (!response.getStatusCode().is2xxSuccessful()) {
                    log.error("Nemotron API returned non-2xx for invoice {}: {}", invoiceId, response.getStatusCode().value());
                    return null;
                }
                return response.getBody();
            } catch (RestClientResponseException e) {
                log.error("Nemotron API request failed for invoice {} on attempt {} with status {}",
                        invoiceId, attempt, e.getStatusCode().value(), e);
            } catch (RestClientException e) {
                log.error("Nemotron API request failed for invoice {} on attempt {}", invoiceId, attempt, e);
            }

            if (attempt < 3) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Nemotron retry sleep interrupted for invoice {}", invoiceId, e);
                    return null;
                }
                delayMs = delayMs * 2;
            }
        }

        return null;
    }

    private ExtractionRunResult failedResult(String invoiceId, int runNumber, long start, String errorMessage) {
        return ExtractionRunResult.builder()
                .invoiceId(invoiceId)
                .modelName(getModelName())
                .runNumber(runNumber)
                .extractionStatus("FAILED")
                .errorMessage(errorMessage)
                .latencyMs((int) (System.currentTimeMillis() - start))
                .build();
    }
}
