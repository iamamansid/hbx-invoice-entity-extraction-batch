package com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor;

import com.research.hbx_invoice_entity_extraction_batch.batch.service.GcsStorageService;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.NormalizationService;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.nemotron.NemotronEntityParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NemotronExtractorTest {

    @Mock
    private NemotronEntityParser entityParser;

    @Mock
    private NormalizationService normalizationService;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private GcsStorageService gcsStorageService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private NemotronExtractor extractor;

    @BeforeEach
    void setUp() {
        extractor = new NemotronExtractor(entityParser, normalizationService, objectMapper, restTemplate, gcsStorageService);
        ReflectionTestUtils.setField(extractor, "apiKey", "secret");
        ReflectionTestUtils.setField(extractor, "apiEndpoint", "https://ai.api.nvidia.com/v1/cv/nvidia/nemotron-ocr-v1");
    }

    @Test
    void extractBuildsCompletedResultFromNemotronResponse() throws Exception {
        JsonNode responseBody = objectMapper.readTree("""
                {
                  "data": [
                    {
                      "index": 0,
                      "text_detections": [
                        {
                          "text_prediction": {
                            "text": "Invoice Number: #ABC98765432100",
                            "confidence": 0.964
                          },
                          "bounding_box": {
                            "points": [
                              {"x": 0.118, "y": 0.124},
                              {"x": 0.451, "y": 0.124},
                              {"x": 0.451, "y": 0.139},
                              {"x": 0.118, "y": 0.139}
                            ]
                          }
                        }
                      ]
                    }
                  ]
                }
                """);

        when(gcsStorageService.downloadBytes("gs://bucket/invoices/sample.png")).thenReturn(new byte[]{1, 2, 3});
        when(restTemplate.exchange(
                eq("https://ai.api.nvidia.com/v1/cv/nvidia/nemotron-ocr-v1"),
                eq(HttpMethod.POST),
                any(),
                eq(JsonNode.class)
        )).thenReturn(ResponseEntity.ok(responseBody));
        when(entityParser.parse(any())).thenReturn(new NemotronEntityParser.ParsedEntities(
                "ABC98765432100",
                java.util.List.of("2024-03-12"),
                java.util.List.of("1250.00"),
                java.util.List.of("Acme Corp")
        ));

        var result = extractor.extract("INV-001", "gs://bucket/invoices/sample.png", 1);

        assertThat(result.getExtractionStatus()).isEqualTo("COMPLETED");
        assertThat(result.getModelName()).isEqualTo("nemotron-ocr-v1");
        assertThat(result.getTokenCount()).isNull();

        JsonNode extractedJson = objectMapper.readTree(result.getExtractedJson());
        assertThat(extractedJson.path("INVOICE_NO").asText()).isEqualTo("ABC98765432100");
        assertThat(extractedJson.path("DATE")).hasSize(1);
        assertThat(extractedJson.path("DATE").get(0).asText()).isEqualTo("2024-03-12");
        assertThat(extractedJson.path("AMOUNT")).hasSize(1);
        assertThat(extractedJson.path("AMOUNT").get(0).asText()).isEqualTo("1250.00");
        assertThat(extractedJson.path("COMPANY")).hasSize(1);
        assertThat(extractedJson.path("COMPANY").get(0).asText()).isEqualTo("Acme Corp");
    }
}
