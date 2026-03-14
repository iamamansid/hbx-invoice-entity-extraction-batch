// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/model/dto/nemotron/NemotronOcrResponse.java
package com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.nemotron;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class NemotronOcrResponse {

    @Builder.Default
    private List<TextDetection> detections = new ArrayList<>();

    public static NemotronOcrResponse from(JsonNode responseBody) {
        try {
            if (responseBody == null || responseBody.isMissingNode()) {
                return NemotronOcrResponse.builder()
                        .detections(new ArrayList<>())
                        .build();
            }

            JsonNode detectionNodes = responseBody.path("data").path(0).path("text_detections");
            if (!detectionNodes.isArray() || detectionNodes.isEmpty()) {
                return NemotronOcrResponse.builder()
                        .detections(new ArrayList<>())
                        .build();
            }

            List<TextDetection> detections = new ArrayList<>();
            for (JsonNode detectionNode : detectionNodes) {
                TextDetection detection = TextDetection.from(detectionNode);
                if (detection != null) {
                    detections.add(detection);
                }
            }

            return NemotronOcrResponse.builder()
                    .detections(detections)
                    .build();
        } catch (Exception e) {
            log.error("Failed to parse Nemotron OCR response", e);
            return NemotronOcrResponse.builder()
                    .detections(new ArrayList<>())
                    .build();
        }
    }

    public List<TextDetection> sortedByReadingOrder() {
        return detections == null
                ? List.of()
                : detections.stream()
                .sorted(Comparator.comparingDouble(TextDetection::getYMin)
                        .thenComparingDouble(TextDetection::getXMin))
                .toList();
    }
}
