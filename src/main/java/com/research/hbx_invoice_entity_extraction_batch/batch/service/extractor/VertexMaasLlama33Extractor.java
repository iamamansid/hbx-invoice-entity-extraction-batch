// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/extractor/VertexMaasLlama33Extractor.java
package com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor;

import com.research.hbx_invoice_entity_extraction_batch.batch.service.VertexMaasTokenService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Order(3)
public class VertexMaasLlama33Extractor extends AbstractLlmExtractor {

    private final VertexMaasTokenService tokenService;

    @Value("${vertex.maas.endpoint-template:https://%s/%s/projects/%s/locations/%s/endpoints/openapi/chat/completions}")
    private String endpointTemplate;

    @Value("${vertex.maas.project-id:${gcp.project-id}}")
    private String projectId;

    @Value("${vertex.maas.llama33.name:llama-3.3-70b}")
    private String modelName;

    @Value("${vertex.maas.llama33.model-id:meta/llama-3.3-70b-instruct-maas}")
    private String modelId;

    @Value("${vertex.maas.llama33.endpoint:us-central1-aiplatform.googleapis.com}")
    private String endpointHost;

    @Value("${vertex.maas.llama33.api-version:v1beta1}")
    private String apiVersion;

    @Value("${vertex.maas.llama33.location:us-central1}")
    private String location;

    @Value("${vertex.maas.llama33.stream:false}")
    private boolean stream;

    @Value("${vertex.maas.llama33.max-tokens:512}")
    private int maxTokens;

    @Value("${vertex.maas.llama33.top-p:0.95}")
    private double topP;

    public VertexMaasLlama33Extractor(
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
        requestBody.put("max_tokens", maxTokens);
        requestBody.put("temperature", getTemperature(getCurrentRunNumber()));
        requestBody.put("top_p", topP);

        Map<String, Object> userMessage = new HashMap<>();
        userMessage.put("role", "user");

        Map<String, String> textPart = new HashMap<>();
        textPart.put("type", "text");
        textPart.put("text", prompt);
        userMessage.put("content", List.of(textPart));

        requestBody.put("messages", List.of(userMessage));

        return new HttpEntity<>(objectMapper.writeValueAsString(requestBody), headers);
    }

    @Override
    protected String getEndpointUrl() {
        return String.format(endpointTemplate, endpointHost, apiVersion, projectId, location);
    }

    @Override
    protected String parseContentFromResponse(String responseBody) throws Exception {
        if (responseBody != null && responseBody.contains("data:")) {
            StringBuilder merged = new StringBuilder();
            for (JsonNode chunk : parseSseChunks(responseBody)) {
                String token = extractContent(chunk);
                if (token != null && !token.isBlank()) {
                    merged.append(token);
                }
            }
            if (!merged.isEmpty()) {
                return merged.toString();
            }
        }

        JsonNode root = objectMapper.readTree(responseBody);
        return extractContent(root);
    }

    @Override
    protected int extractTokenCount(String responseBody) throws Exception {
        if (responseBody != null && responseBody.contains("data:")) {
            int totalTokens = 0;
            for (JsonNode chunk : parseSseChunks(responseBody)) {
                int chunkTokens = chunk.path("usage").path("total_tokens").asInt(0);
                if (chunkTokens > 0) {
                    totalTokens = chunkTokens;
                }
            }
            return totalTokens;
        }

        JsonNode root = objectMapper.readTree(responseBody);
        return root.path("usage").path("total_tokens").asInt(0);
    }

    private List<JsonNode> parseSseChunks(String responseBody) {
        List<JsonNode> chunks = new ArrayList<>();
        String[] lines = responseBody.split("\\R");
        for (String line : lines) {
            String trimmed = line.trim();
            if (!trimmed.startsWith("data:")) {
                continue;
            }

            String payload = trimmed.substring("data:".length()).trim();
            if (payload.isEmpty() || "[DONE]".equals(payload)) {
                continue;
            }

            try {
                chunks.add(objectMapper.readTree(payload));
            } catch (Exception ignored) {
                // Ignore non-JSON stream lines and continue parsing valid chunks.
            }
        }
        return chunks;
    }

    private String extractContent(JsonNode root) {
        JsonNode choices = root.path("choices");
        if (!choices.isArray() || choices.isEmpty()) {
            return null;
        }

        JsonNode choice = choices.get(0);
        String deltaContent = choice.path("delta").path("content").asText(null);
        if (deltaContent != null && !deltaContent.isBlank()) {
            return deltaContent;
        }

        JsonNode messageContentNode = choice.path("message").path("content");
        if (messageContentNode.isTextual()) {
            String content = messageContentNode.asText(null);
            if (content != null && !content.isBlank()) {
                return content;
            }
        }

        if (messageContentNode.isArray()) {
            StringBuilder merged = new StringBuilder();
            for (JsonNode part : messageContentNode) {
                String text = part.path("text").asText("");
                if (!text.isEmpty()) {
                    merged.append(text);
                }
            }
            if (!merged.isEmpty()) {
                return merged.toString();
            }
        }

        return null;
    }

}
