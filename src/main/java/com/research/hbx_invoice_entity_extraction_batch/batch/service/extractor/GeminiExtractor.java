// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/extractor/GeminiExtractor.java
package com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
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
    protected String getEndpointUrl() {
        // API-key based Vertex pattern:
        // https://aiplatform.googleapis.com/v1/publishers/google/models/{model}:streamGenerateContent?key=...
        return String.format("https://%s/v1/publishers/google/models/%s:streamGenerateContent?key=%s",
                endpoint, modelName, apiKey);
    }

    @Override
    protected HttpEntity<String> buildRequestEntity(String prompt) throws Exception {
        HttpHeaders headers = createJsonHeaders();

        Map<String, Object> requestMap = new HashMap<>();
        Map<String, Object> contents = new HashMap<>();
        contents.put("role", "user");
        
        Map<String, Object> parts = new HashMap<>();
        parts.put("text", prompt);
        
        contents.put("parts", List.of(parts));
        requestMap.put("contents", List.of(contents));

        // Enforce JSON output for Gemini
        requestMap.put("generationConfig", Map.of(
                "temperature", getTemperature(getCurrentRunNumber()),
                "maxOutputTokens", 1024,
                "responseMimeType", "application/json"
        ));

        return new HttpEntity<>(objectMapper.writeValueAsString(requestMap), headers);
    }

    @Override
    protected String parseContentFromResponse(String responseBody) throws Exception {
        JsonNode root = objectMapper.readTree(responseBody);

        if (root.isArray()) {
            StringBuilder merged = new StringBuilder();
            for (JsonNode chunk : root) {
                JsonNode candidates = chunk.path("candidates");
                if (candidates.isArray() && !candidates.isEmpty()) {
                    JsonNode parts = candidates.get(0).path("content").path("parts");
                    if (parts.isArray()) {
                        for (JsonNode part : parts) {
                            String text = part.path("text").asText("");
                            if (!text.isEmpty()) {
                                merged.append(text);
                            }
                        }
                    }
                }
            }
            return merged.isEmpty() ? null : merged.toString();
        }

        JsonNode candidates = root.path("candidates");
        if (candidates.isArray() && !candidates.isEmpty()) {
            JsonNode parts = candidates.get(0).path("content").path("parts");
            if (parts.isArray() && !parts.isEmpty()) {
                return parts.get(0).path("text").asText();
            }
        }
        return null;
    }

    @Override
    protected int extractTokenCount(String responseBody) throws Exception {
        JsonNode root = objectMapper.readTree(responseBody);
        if (root.isArray()) {
            for (int i = root.size() - 1; i >= 0; i--) {
                int count = root.get(i).path("usageMetadata").path("totalTokenCount").asInt(0);
                if (count > 0) {
                    return count;
                }
            }
            return 0;
        }
        return root.path("usageMetadata").path("totalTokenCount").asInt(0);
    }
}
