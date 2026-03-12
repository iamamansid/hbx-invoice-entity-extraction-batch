// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/extractor/MistralExtractor.java
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
@Order(5)
public class MistralExtractor extends AbstractLlmExtractor {

    @Value("${mistral.api.endpoint}")
    private String endpointUrl;

    @Value("${mistral.api.key}")
    private String apiKey;

    @Value("${mistral.api.model:mistral-medium-latest}")
    private String model;

    @Value("${mistral.api.name:mistral-medium}")
    private String modelName;

    public MistralExtractor(RestTemplate restTemplate, ObjectMapper objectMapper) {
        super(restTemplate, objectMapper);
    }

    @Override
    public String getModelName() {
        return modelName;
    }

    @Override
    protected String getEndpointUrl() {
        return endpointUrl;
    }

    @Override
    protected HttpEntity<String> buildRequestEntity(String prompt) throws Exception {
        HttpHeaders headers = createJsonHeaders();
        headers.setBearerAuth(apiKey);

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("model", model);
        requestBody.put("temperature", getTemperature(getCurrentRunNumber()));
        requestBody.put("max_tokens", 1024);
        
        // System and User messages
        Map<String, String> userMessage = new HashMap<>();
        userMessage.put("role", "user");
        userMessage.put("content", prompt);
        
        requestBody.put("messages", List.of(userMessage));

        // Request strict JSON output for structured extraction.
        Map<String, String> responseFormat = new HashMap<>();
        responseFormat.put("type", "json_object");
        requestBody.put("response_format", responseFormat);

        return new HttpEntity<>(objectMapper.writeValueAsString(requestBody), headers);
    }

    @Override
    protected String parseContentFromResponse(String responseBody) throws Exception {
        JsonNode root = objectMapper.readTree(responseBody);
        JsonNode choices = root.path("choices");
        if (choices.isArray() && !choices.isEmpty()) {
            return choices.get(0).path("message").path("content").asText();
        }
        return null;
    }

    @Override
    protected int extractTokenCount(String responseBody) throws Exception {
        JsonNode root = objectMapper.readTree(responseBody);
        return root.path("usage").path("total_tokens").asInt(0);
    }
}
