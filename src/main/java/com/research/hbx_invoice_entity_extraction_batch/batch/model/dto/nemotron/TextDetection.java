// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/model/dto/nemotron/TextDetection.java
package com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.nemotron;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.regex.Pattern;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TextDetection {

    private static final Pattern AMOUNT_PATTERN = Pattern.compile("^\\d{1,3}(,\\d{3})*\\.\\d{2}$");
    private static final Pattern DATE_PATTERN = Pattern.compile(
            "^(?:(?:0?[1-9]|[12]\\d|3[01])[./-](?:0?[1-9]|1[0-2])[./-](?:19|20)\\d{2}|(?:19|20)\\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\\d|3[01])|(?:0?[1-9]|1[0-2])[/-](?:0?[1-9]|[12]\\d|3[01])[/-](?:19|20)\\d{2})$"
    );

    private String text;
    private double confidence;
    private double xMin;
    private double yMin;
    private double xMax;
    private double yMax;

    public static TextDetection from(JsonNode detectionNode) {
        if (detectionNode == null || detectionNode.isMissingNode()) {
            return null;
        }

        JsonNode textPrediction = detectionNode.path("text_prediction");
        String text = textPrediction.path("text").asText(null);
        if (text == null || text.isBlank()) {
            return null;
        }

        JsonNode points = detectionNode.path("bounding_box").path("points");
        JsonNode topLeft = points.isArray() && points.size() > 0 ? points.get(0) : null;
        JsonNode bottomRight = points.isArray() && points.size() > 2 ? points.get(2) : null;

        return TextDetection.builder()
                .text(text)
                .confidence(textPrediction.path("confidence").asDouble(0.0))
                .xMin(topLeft == null ? 0.0 : topLeft.path("x").asDouble(0.0))
                .yMin(topLeft == null ? 0.0 : topLeft.path("y").asDouble(0.0))
                .xMax(bottomRight == null ? 0.0 : bottomRight.path("x").asDouble(0.0))
                .yMax(bottomRight == null ? 0.0 : bottomRight.path("y").asDouble(0.0))
                .build();
    }

    public boolean isInXRange(double xMinBound, double xMaxBound) {
        return xMin >= xMinBound && xMax <= xMaxBound;
    }

    public boolean isBelowY(double yThreshold) {
        return yMin > yThreshold;
    }

    public boolean looksLikeAmount() {
        return text != null && AMOUNT_PATTERN.matcher(text.trim()).matches();
    }

    public boolean looksLikeDate() {
        return text != null && DATE_PATTERN.matcher(text.trim()).matches();
    }
}
