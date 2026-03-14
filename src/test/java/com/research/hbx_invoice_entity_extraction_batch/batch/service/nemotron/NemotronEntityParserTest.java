package com.research.hbx_invoice_entity_extraction_batch.batch.service.nemotron;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.nemotron.NemotronOcrResponse;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.nemotron.TextDetection;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.NormalizationService;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class NemotronEntityParserTest {

    private final NemotronEntityParser parser = new NemotronEntityParser(new NormalizationService());

    @Test
    void parseExtractsEntitiesUsingLabelsAndAmountColumn() {
        NemotronOcrResponse response = NemotronOcrResponse.builder()
                .detections(List.of(
                        detection("Invoice Number: #abc98765432100", 0.95, 0.10, 0.10, 0.45, 0.13),
                        detection("Invoice Date: 2024-03-12", 0.92, 0.10, 0.16, 0.35, 0.19),
                        detection("2024-03-15", 0.90, 0.10, 0.22, 0.20, 0.25),
                        detection("Date: 13/03/2024", 0.68, 0.10, 0.28, 0.25, 0.31),
                        detection("Total: $1,250.00", 0.94, 0.60, 0.16, 0.85, 0.19),
                        detection("Amount", 0.99, 0.70, 0.10, 0.80, 0.13),
                        detection("75.50", 0.91, 0.71, 0.21, 0.79, 0.24),
                        detection("Vendor: Acme Corp", 0.93, 0.10, 0.34, 0.30, 0.37)
                ))
                .build();

        NemotronEntityParser.ParsedEntities parsed = parser.parse(response);

        assertThat(parsed.invoiceNo()).isEqualTo("ABC98765432100");
        assertThat(parsed.dates()).containsExactly("2024-03-12", "2024-03-15");
        assertThat(parsed.amounts()).containsExactly("1250.00", "75.50");
        assertThat(parsed.companies()).containsExactly("Acme Corp");
    }

    private TextDetection detection(String text, double confidence, double xMin, double yMin, double xMax, double yMax) {
        return TextDetection.builder()
                .text(text)
                .confidence(confidence)
                .xMin(xMin)
                .yMin(yMin)
                .xMax(xMax)
                .yMax(yMax)
                .build();
    }
}
