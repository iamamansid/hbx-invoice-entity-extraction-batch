// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/extractor/GeminiExtractor.java
/*
 * BEFORE RE-RUNNING THE PIPELINE after this fix:
 * Run the following SQL on your PostgreSQL instance:
 *
 * -- Clear only Gemini failed runs (preserves other models' data)
 * DELETE FROM evaluation_result
 *   WHERE invoice_id IN (
 *     SELECT DISTINCT invoice_id FROM extraction_run
 *     WHERE model_name = 'gemini-2.5-pro'
 *   );
 * DELETE FROM extraction_run WHERE model_name = 'gemini-2.5-pro';
 *
 * -- Reset invoice extraction status so batch re-reads them
 * UPDATE invoice
 * SET extraction_status = NULL
 * WHERE invoice_id IN (
 *   SELECT DISTINCT invoice_id FROM extraction_run
 *   WHERE model_name != 'gemini-2.5-pro'
 *   -- Only reset invoices where Gemini data is now missing
 * );
 *
 * -- Simpler alternative: full reset (if you want clean run for all):
 * TRUNCATE TABLE evaluation_result;
 * TRUNCATE TABLE extraction_run;
 * UPDATE invoice SET extraction_status = NULL, evaluation_status = NULL;
 */
package com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionRunResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
@Order(1)
public class GeminiExtractor extends AbstractLlmExtractor {

    @Value("${vertex.ai.endpoint}")
    private String endpoint;
    
    @Value("${vertex.ai.gemini-pro-model:gemini-2.5-pro}")
    private String modelName;

    @Value("${vertex.ai.api-key}")
    private String apiKey;

    public GeminiExtractor(RestTemplate restTemplate, ObjectMapper objectMapper) {
        super(restTemplate, objectMapper);
    }

    @Override
    public String getModelName() {
        return modelName;
    }

    @Override
    public ExtractionRunResult extract(String invoiceId, String text, int runNumber) {
        long start = System.currentTimeMillis();
        String prompt = PROMPT_TEMPLATE.replace("{{text}}", text != null ? text : "");

        try {
            long latency;
            JsonNode responseJson;
            try {
                String responseBody = invokeGeminiWithRetry(prompt, runNumber);
                latency = System.currentTimeMillis() - start;
                responseJson = objectMapper.readTree(responseBody);
                logResponseStructure(invoiceId, responseJson);
            } catch (Exception e) {
                return ExtractionRunResult.builder()
                        .invoiceId(invoiceId)
                        .modelName(getModelName())
                        .runNumber(runNumber)
                        .extractionStatus("FAILED")
                        .errorMessage(e.getMessage())
                        .latencyMs((int) (System.currentTimeMillis() - start))
                        .build();
            }

            String rawText = extractGeminiText(responseJson);
            if (rawText == null) {
                return ExtractionRunResult.builder()
                        .invoiceId(invoiceId)
                        .modelName(getModelName())
                        .runNumber(runNumber)
                        .extractionStatus("FAILED")
                        .errorMessage("No text in Gemini response")
                        .latencyMs((int) latency)
                        .build();
            }

            try {
                objectMapper.readTree(rawText);
            } catch (Exception e) {
                return ExtractionRunResult.builder()
                        .invoiceId(invoiceId)
                        .modelName(getModelName())
                        .runNumber(runNumber)
                        .extractionStatus("PARSE_FAILED")
                        .errorMessage(truncate(rawText))
                        .latencyMs((int) latency)
                        .build();
            }

            int tokenCount = extractTokenCount(responseJson);

            return ExtractionRunResult.builder()
                    .invoiceId(invoiceId)
                    .modelName(getModelName())
                    .runNumber(runNumber)
                    .extractedJson(rawText)
                    .latencyMs((int) latency)
                    .tokenCount(tokenCount)
                    .extractionStatus("COMPLETED")
                    .build();
        } catch (Exception e) {
            log.error("{} API call failed for invoice {}", getModelName(), invoiceId, e);
            return ExtractionRunResult.builder()
                    .invoiceId(invoiceId)
                    .modelName(getModelName())
                    .runNumber(runNumber)
                    .extractionStatus("FAILED")
                    .errorMessage(e.getMessage())
                    .latencyMs((int) (System.currentTimeMillis() - start))
                    .build();
        }
    }

    @Override
    protected String getEndpointUrl() {
        // API-key based Vertex pattern:
        // https://aiplatform.googleapis.com/v1/publishers/google/models/{model}:streamGenerateContent?key=...
        return String.format("https://%s/v1/publishers/google/models/%s:streamGenerateContent?key=%s",
                endpoint, modelName, apiKey);
    }

    @Override
    protected HttpEntity<String> buildRequestEntity(String prompt) throws Exception {
        return buildRequestEntity(prompt, getCurrentRunNumber());
    }

    private HttpEntity<String> buildRequestEntity(String prompt, int runNumber) throws Exception {
        HttpHeaders headers = createJsonHeaders();

        Map<String, Object> requestMap = new HashMap<>();
        Map<String, Object> contents = new HashMap<>();
        contents.put("role", "user");
        
        Map<String, Object> parts = new HashMap<>();
        parts.put("text", prompt);
        
        contents.put("parts", List.of(parts));
        requestMap.put("contents", List.of(contents));

        Map<String, Object> generationConfig = new LinkedHashMap<>();
        generationConfig.put("temperature", getTemperature(runNumber));
        generationConfig.put("maxOutputTokens", 8192);
        generationConfig.put("responseMimeType", "application/json");
        generationConfig.put("thinkingConfig", Map.of("thinkingBudget", 512));
        requestMap.put("generationConfig", generationConfig);

        return new HttpEntity<>(objectMapper.writeValueAsString(requestMap), headers);
    }

    @Override
    protected String parseContentFromResponse(String responseBody) throws Exception {
        return extractGeminiText(objectMapper.readTree(responseBody));
    }

    @Override
    protected int extractTokenCount(String responseBody) throws Exception {
        return extractTokenCount(objectMapper.readTree(responseBody));
    }

    private String extractGeminiText(JsonNode responseBody) {
        try {
            if (responseBody == null || responseBody.isNull() || responseBody.isMissingNode()) {
                return null;
            }

            if (responseBody.isArray()) {
                return extractGeminiTextFromStreamChunks(responseBody);
            }

            return extractGeminiTextFromSinglePayload(responseBody);
        } catch (Exception e) {
            log.error("Failed to extract Gemini response text", e);
            return null;
        }
    }

    private String extractGeminiTextFromSinglePayload(JsonNode responseBody) {
        JsonNode candidates = responseBody.path("candidates");
        if (!candidates.isArray() || candidates.isEmpty()) {
            logMissingCandidates(responseBody);
            return null;
        }
        return extractGeminiTextFromCandidate(candidates.get(0));
    }

    private String extractGeminiTextFromStreamChunks(JsonNode responseBody) {
        StringBuilder merged = new StringBuilder();
        int candidateChunks = 0;
        String terminalFinishReason = "STOP";

        for (JsonNode chunk : responseBody) {
            JsonNode candidates = chunk.path("candidates");
            if (!candidates.isArray() || candidates.isEmpty()) {
                continue;
            }

            JsonNode candidate = candidates.get(0);
            candidateChunks++;

            String finishReason = candidate.path("finishReason").asText("");
            if (!finishReason.isBlank()) {
                terminalFinishReason = finishReason;
            }

            appendTextFromParts(candidate, merged);
        }

        if (candidateChunks == 0) {
            logMissingCandidates(responseBody);
            return null;
        }

        if ("SAFETY".equals(terminalFinishReason) || "RECITATION".equals(terminalFinishReason)) {
            log.warn("Gemini blocked streamed response: finishReason={}", terminalFinishReason);
            return null;
        }

        if (merged.isEmpty()) {
            log.warn("Gemini stream response had {} candidate chunks but no non-thought text", candidateChunks);
            return null;
        }

        String mergedText = merged.toString();
        if ("MAX_TOKENS".equals(terminalFinishReason)) {
            log.warn("Gemini stream hit MAX_TOKENS - attempting partial recovery");
            return recoverPartialJson(mergedText);
        }

        return mergedText;
    }

    private String extractGeminiTextFromCandidate(JsonNode candidate) {
        String finishReason = candidate.path("finishReason").asText("STOP");
        if ("MAX_TOKENS".equals(finishReason)) {
            log.warn("Gemini hit MAX_TOKENS - attempting partial recovery");
            String partial = extractTextFromParts(candidate);
            return recoverPartialJson(partial);
        }

        if ("SAFETY".equals(finishReason) || "RECITATION".equals(finishReason)) {
            log.warn("Gemini blocked response: finishReason={}", finishReason);
            return null;
        }

        return extractTextFromParts(candidate);
    }

    /**
     * Iterates through all parts and returns the last non-thought text.
     * Gemini 2.5 Pro thinking mode can put thought content first and the actual response last.
     */
    private String extractTextFromParts(JsonNode candidate) {
        JsonNode parts = candidate.path("content").path("parts");
        if (parts.isMissingNode() || !parts.isArray() || parts.isEmpty()) {
            log.warn("Gemini candidate has no parts");
            return null;
        }

        String lastNonThoughtText = null;
        for (JsonNode part : parts) {
            boolean isThought = part.path("thought").asBoolean(false);
            if (isThought) {
                log.debug("Skipping thought part ({} chars)", part.path("text").asText("").length());
                continue;
            }

            String text = part.path("text").asText(null);
            if (text != null && !text.isBlank()) {
                lastNonThoughtText = text;
            }
        }

        if (lastNonThoughtText == null) {
            log.warn("No non-thought text found in {} parts", parts.size());
        }

        return lastNonThoughtText;
    }

    private void appendTextFromParts(JsonNode candidate, StringBuilder merged) {
        JsonNode parts = candidate.path("content").path("parts");
        if (parts.isMissingNode() || !parts.isArray() || parts.isEmpty()) {
            return;
        }

        for (JsonNode part : parts) {
            boolean isThought = part.path("thought").asBoolean(false);
            if (isThought) {
                continue;
            }

            String text = part.path("text").asText(null);
            if (text != null && !text.isBlank()) {
                merged.append(text);
            }
        }
    }

    private int extractTokenCount(JsonNode responseBody) {
        if (responseBody == null || responseBody.isNull() || responseBody.isMissingNode()) {
            return 0;
        }

        if (responseBody.isArray()) {
            for (int i = responseBody.size() - 1; i >= 0; i--) {
                JsonNode usageMetadata = responseBody.get(i).path("usageMetadata");
                int candidatesTokenCount = usageMetadata.path("candidatesTokenCount").asInt(0);
                if (candidatesTokenCount > 0) {
                    return candidatesTokenCount;
                }
                int totalTokenCount = usageMetadata.path("totalTokenCount").asInt(0);
                if (totalTokenCount > 0) {
                    return totalTokenCount;
                }
            }
            return 0;
        }

        JsonNode usageMetadata = responseBody.path("usageMetadata");
        int candidatesTokenCount = usageMetadata.path("candidatesTokenCount").asInt(0);
        if (candidatesTokenCount > 0) {
            return candidatesTokenCount;
        }
        return usageMetadata.path("totalTokenCount").asInt(0);
    }

    private void logResponseStructure(String invoiceId, JsonNode responseJson) {
        if (responseJson.isArray()) {
            JsonNode lastChunk = responseJson.isEmpty() ? null : responseJson.get(responseJson.size() - 1);
            log.debug("Gemini raw stream response for invoice {}: chunks={}, lastChunkCandidates={}, lastChunkPartCount={}",
                    invoiceId,
                    responseJson.size(),
                    lastChunk == null ? 0 : lastChunk.path("candidates").size(),
                    lastChunk == null ? 0 : lastChunk.path("candidates").path(0).path("content").path("parts").size());
            return;
        }

        log.debug("Gemini raw response structure for invoice {}: candidates={}, firstCandidatePartCount={}",
                invoiceId,
                responseJson.path("candidates").size(),
                responseJson.path("candidates").path(0).path("content").path("parts").size());
    }

    private void logMissingCandidates(JsonNode responseBody) {
        String rawResponse = responseBody.toString();
        log.error("Gemini response has no candidates. Full response: {}",
                rawResponse.substring(0, Math.min(500, rawResponse.length())));
    }

    private String recoverPartialJson(String truncated) {
        if (truncated == null || truncated.isBlank()) {
            return null;
        }

        try {
            objectMapper.readTree(truncated);
            return truncated;
        } catch (Exception ignored) {
        }

        String trimmed = truncated.trim();
        int lastComplete = -1;
        for (int i = trimmed.length() - 1; i >= 0; i--) {
            char c = trimmed.charAt(i);
            if (c == ']' || (c == '"' && i > 0)) {
                lastComplete = i;
                break;
            }
        }

        if (lastComplete > 0) {
            int lastComma = trimmed.lastIndexOf(",", lastComplete);
            String candidate = lastComma > 0
                    ? trimmed.substring(0, lastComma) + "}"
                    : trimmed.substring(0, lastComplete + 1) + "}";

            try {
                objectMapper.readTree(candidate);
                log.info("Partial JSON successfully recovered ({} chars)", candidate.length());
                return candidate;
            } catch (Exception e) {
                log.debug("Partial JSON recovery failed: {}", e.getMessage());
            }
        }

        log.warn("Could not recover partial JSON from truncated response");
        return null;
    }

    private String invokeGeminiWithRetry(String prompt, int runNumber) throws Exception {
        Exception lastException = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                HttpEntity<String> requestEntity = buildRequestEntity(prompt, runNumber);
                ResponseEntity<String> response = restTemplate.exchange(
                        getEndpointUrl(),
                        HttpMethod.POST,
                        requestEntity,
                        String.class
                );
                return response.getBody();
            } catch (RestClientResponseException e) {
                String details = String.format(
                        "HTTP %d from %s endpoint: %s",
                        e.getStatusCode().value(),
                        getModelName(),
                        truncate(e.getResponseBodyAsString())
                );
                lastException = new RuntimeException(details, e);
                log.error("Attempt {} failed for {}: {}", attempt, getModelName(), details);
                if (e.getStatusCode().is4xxClientError()) {
                    break;
                }
            } catch (RestClientException e) {
                lastException = e;
                log.error("Attempt {} failed for {}: {}", attempt, getModelName(), e.getMessage());
                if (attempt == 3) {
                    break;
                }
                Thread.sleep(2000);
            }
        }
        throw new RuntimeException("LLM API call failed after 3 attempts", lastException);
    }

    private String truncate(String text) {
        if (text == null) {
            return "";
        }
        return text.length() <= 500 ? text : text.substring(0, 500);
    }
}
