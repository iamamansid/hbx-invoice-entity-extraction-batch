// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/extractor/VertexMaasLlama4Extractor.java
package com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.VertexMaasTokenService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Order(4)
public class VertexMaasLlama4Extractor extends AbstractLlmExtractor {

    private final VertexMaasTokenService tokenService;

    @Value("${vertex.maas.endpoint-template:https://%s/%s/projects/%s/locations/%s/endpoints/openapi/chat/completions}")
    private String endpointTemplate;

    @Value("${vertex.maas.project-id:${gcp.project-id}}")
    private String projectId;

    @Value("${vertex.maas.llama4.name:llama-4-maverick}")
    private String modelName;

    @Value("${vertex.maas.llama4.model-id:meta/llama-4-maverick-17b-128e-instruct-maas}")
    private String modelId;

    @Value("${vertex.maas.llama4.endpoint:us-east5-aiplatform.googleapis.com}")
    private String endpointHost;

    @Value("${vertex.maas.llama4.api-version:v1}")
    private String apiVersion;

    @Value("${vertex.maas.llama4.location:us-east5}")
    private String location;

    @Value("${vertex.maas.llama4.stream:false}")
    private boolean stream;

    public VertexMaasLlama4Extractor(
            RestTemplate restTemplate,
            ObjectMapper objectMapper,
            VertexMaasTokenService tokenService) {
        super(restTemplate, objectMapper);
        this.tokenService = tokenService;
    }

    @Override
    public String getModelName() {
        return modelName;
    }

    @Override
    protected HttpEntity<String> buildRequestEntity(String prompt) throws Exception {
        HttpHeaders headers = createJsonHeaders();
        headers.setBearerAuth(tokenService.getFreshAccessToken());

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("model", modelId);
        requestBody.put("stream", stream);
        requestBody.put("temperature", getTemperature(getCurrentRunNumber()));
        requestBody.put("max_tokens", 1024);

        Map<String, String> userMessage = new HashMap<>();
        userMessage.put("role", "user");
        userMessage.put("content", prompt);
        requestBody.put("messages", List.of(userMessage));

        return new HttpEntity<>(objectMapper.writeValueAsString(requestBody), headers);
    }

    @Override
    protected String getEndpointUrl() {
        return String.format(endpointTemplate, endpointHost, apiVersion, projectId, location);
    }

    @Override
    protected String parseContentFromResponse(String responseBody) throws Exception {
        JsonNode root = objectMapper.readTree(responseBody);
        JsonNode choices = root.path("choices");
        if (choices.isArray() && !choices.isEmpty()) {
            JsonNode messageContent = choices.get(0).path("message").path("content");
            if (messageContent.isTextual()) {
                return messageContent.asText();
            }
            if (messageContent.isArray() && !messageContent.isEmpty()) {
                StringBuilder merged = new StringBuilder();
                for (JsonNode part : messageContent) {
                    String text = part.path("text").asText("");
                    if (!text.isEmpty()) {
                        merged.append(text);
                    }
                }
                return merged.isEmpty() ? null : merged.toString();
            }
        }
        return null;
    }

    @Override
    protected int extractTokenCount(String responseBody) throws Exception {
        JsonNode root = objectMapper.readTree(responseBody);
        return root.path("usage").path("total_tokens").asInt(0);
    }

}
