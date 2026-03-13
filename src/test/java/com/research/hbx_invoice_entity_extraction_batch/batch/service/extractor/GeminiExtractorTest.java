package com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.RestTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class GeminiExtractorTest {

    private GeminiExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new GeminiExtractor(mock(RestTemplate.class), new ObjectMapper());
    }

    @Test
    void parseContentFromResponseReturnsLastNonThoughtPart() throws Exception {
        String responseBody = """
                {
                  "candidates": [
                    {
                      "finishReason": "STOP",
                      "content": {
                        "parts": [
                          { "thought": true, "text": "reasoning" },
                          { "text": "{\\"invoice_no\\":\\"INV-older\\"}" },
                          { "text": "{\\"invoice_no\\":\\"INV-final\\"}" }
                        ]
                      }
                    }
                  ]
                }
                """;

        String extracted = extractor.parseContentFromResponse(responseBody);

        assertThat(extracted).isEqualTo("{\"invoice_no\":\"INV-final\"}");
    }

    @Test
    void parseContentFromResponseReturnsNullForSafetyBlockedResponses() throws Exception {
        String responseBody = """
                {
                  "candidates": [
                    {
                      "finishReason": "SAFETY",
                      "content": {
                        "parts": [
                          { "text": "{\\"invoice_no\\":\\"INV-ignored\\"}" }
                        ]
                      }
                    }
                  ]
                }
                """;

        String extracted = extractor.parseContentFromResponse(responseBody);

        assertThat(extracted).isNull();
    }

    @Test
    void parseContentFromResponseRecoversPartialJsonWhenMaxTokensIsHit() throws Exception {
        String responseBody = """
                {
                  "candidates": [
                    {
                      "finishReason": "MAX_TOKENS",
                      "content": {
                        "parts": [
                          { "thought": true, "text": "reasoning" },
                          { "text": "{\\"invoice_no\\":\\"INV-123\\",\\"dates\\":[\\"2024-01-01\\"],\\"company\\":\\"Acme\\"" }
                        ]
                      }
                    }
                  ]
                }
                """;

        String extracted = extractor.parseContentFromResponse(responseBody);

        assertThat(extracted).isEqualTo("{\"invoice_no\":\"INV-123\",\"dates\":[\"2024-01-01\"]}");
    }
}
