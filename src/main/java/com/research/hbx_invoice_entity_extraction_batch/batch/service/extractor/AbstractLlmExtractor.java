// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/extractor/AbstractLlmExtractor.java
package com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionRunResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

@Slf4j
public abstract class AbstractLlmExtractor implements Extractor {

    protected final RestTemplate restTemplate;
    protected final ObjectMapper objectMapper;
    private final ThreadLocal<Integer> currentRunNumber = new ThreadLocal<>();

    protected static final String PROMPT_TEMPLATE = """
            You are a structured data extraction assistant.
            Extract the following fields from the invoice text below.
            Return ONLY a valid JSON object. No explanation, 
            no markdown, no preamble.
            
            Fields to extract:
            - INVOICE_NO: string (single value)
            - DATE: array of strings in yyyy-MM-dd format
            - AMOUNT: array of strings (numeric only, e.g. "1250.50")
            - COMPANY: array of strings (company names only)
            
            Invoice text:
            {{text}}
            
            Return format:
            {"INVOICE_NO": "...", "DATE": ["..."], "AMOUNT": ["..."], "COMPANY": ["..."]}
            """;

    protected AbstractLlmExtractor(RestTemplate restTemplate, ObjectMapper objectMapper) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public ExtractionRunResult extract(String invoiceId, String text, int runNumber) {
        long start = System.currentTimeMillis();
        String prompt = PROMPT_TEMPLATE.replace("{{text}}", text != null ? text : "");
        currentRunNumber.set(runNumber);

        try {
            long latency;
            String responseBody;
            try {
                responseBody = invokeLlmWithRetry(prompt);
                latency = System.currentTimeMillis() - start;
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

            String extractedContent = parseContentFromResponse(responseBody);
            // Clean markdown if the LLM leaked it despite instructions
            if (extractedContent != null && extractedContent.startsWith("```json")) {
                extractedContent = extractedContent.replaceFirst("```json", "").replaceFirst("```$", "").trim();
            } else if (extractedContent != null && extractedContent.startsWith("```")) {
                extractedContent = extractedContent.replaceFirst("```", "").replaceFirst("```$", "").trim();
            }

            if (extractedContent == null || extractedContent.isBlank()) {
                return ExtractionRunResult.builder()
                        .invoiceId(invoiceId)
                        .modelName(getModelName())
                        .runNumber(runNumber)
                        .extractionStatus("PARSE_FAILED")
                        .errorMessage(responseBody)
                        .latencyMs((int) latency)
                        .build();
            }

            // Validate JSON
            try {
                objectMapper.readTree(extractedContent);
            } catch (JsonProcessingException e) {
                return ExtractionRunResult.builder()
                        .invoiceId(invoiceId)
                        .modelName(getModelName())
                        .runNumber(runNumber)
                        .extractionStatus("PARSE_FAILED")
                        .errorMessage(responseBody)
                        .latencyMs((int) latency)
                        .build();
            }

            int tokenCount = extractTokenCount(responseBody);

            return ExtractionRunResult.builder()
                    .invoiceId(invoiceId)
                    .modelName(getModelName())
                    .runNumber(runNumber)
                    .extractedJson(extractedContent)
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
        } finally {
            currentRunNumber.remove();
        }
    }

    private String invokeLlmWithRetry(String prompt) throws Exception {
        Exception lastException = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                HttpEntity<String> requestEntity = buildRequestEntity(prompt);
                ResponseEntity<String> response = restTemplate.exchange(
                        getEndpointUrl(),
                        HttpMethod.POST,
                        requestEntity,
                        String.class
                );
                return response.getBody();
            } catch (RestClientResponseException e) {
                // Include provider response details to make model/key/config issues visible in logs.
                String details = String.format(
                        "HTTP %d from %s endpoint: %s",
                        e.getStatusCode().value(),
                        getModelName(),
                        truncate(e.getResponseBodyAsString())
                );
                lastException = new RuntimeException(details, e);
                log.error("Attempt {} failed for {}: {}", attempt, getModelName(), details);
                if (e.getStatusCode().is4xxClientError()) {
                    break; // Do not retry permanent client errors.
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

    private String truncate(String s) {
        if (s == null) {
            return "";
        }
        int max = 500;
        return s.length() <= max ? s : s.substring(0, max) + "...";
    }

    protected HttpHeaders createJsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    protected int getCurrentRunNumber() {
        Integer runNumber = currentRunNumber.get();
        return runNumber == null ? 1 : runNumber;
    }

    protected abstract HttpEntity<String> buildRequestEntity(String prompt) throws Exception;
    protected abstract String getEndpointUrl();
    protected abstract String parseContentFromResponse(String responseBody) throws Exception;
    protected abstract int extractTokenCount(String responseBody) throws Exception;
}
