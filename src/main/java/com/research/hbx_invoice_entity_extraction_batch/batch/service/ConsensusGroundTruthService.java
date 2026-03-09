// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/ConsensusGroundTruthService.java
package com.research.hbx_invoice_entity_extraction_batch.batch.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.ExtractionRun;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConsensusGroundTruthService {

    private static final Set<String> TARGET_MODELS = Set.of("gemini", "llama", "mistral", "regex");
    private static final DateTimeFormatter DATE_OUTPUT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final List<DateTimeFormatter> DATE_INPUT_FORMATS = List.of(
            DateTimeFormatter.ofPattern("dd.MM.yyyy"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy"),
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy")
    );

    private final ObjectMapper objectMapper;

    public ConsensusResult computeConsensus(String invoiceId, List<ExtractionRun> runs) {
        if (runs == null || runs.isEmpty()) {
            return new ConsensusResult(invoiceId, null, Set.of(), Set.of(), Set.of(), 0, 0);
        }

        Map<String, ExtractionRun> runByModel = new LinkedHashMap<>();
        for (ExtractionRun run : runs) {
            if (run == null || run.getRunNumber() == null || run.getRunNumber() != 1) {
                continue;
            }
            if (!"COMPLETED".equalsIgnoreCase(run.getExtractionStatus())) {
                continue;
            }
            String canonical = canonicalModel(run.getModelName());
            if (!TARGET_MODELS.contains(canonical)) {
                continue;
            }
            // Keep one run per model for run_number=1.
            runByModel.putIfAbsent(canonical, run);
        }

        int modelsAgreed = runByModel.size();
        if (modelsAgreed == 0) {
            return new ConsensusResult(invoiceId, null, Set.of(), Set.of(), Set.of(), 0, 0);
        }

        String consensusInvoiceNo = computeInvoiceNoConsensus(runByModel.values());
        Set<String> consensusDates = computeSetConsensus(runByModel.values(), "DATE", this::normalizeDateValue);
        Set<String> consensusAmounts = computeSetConsensus(runByModel.values(), "AMOUNT", this::normalizeAmountValue);
        Set<String> consensusCompanies = computeCompanyConsensus(runByModel.values());

        int fieldsWithConsensus = 0;
        if (consensusInvoiceNo != null) {
            fieldsWithConsensus++;
        }
        if (!consensusDates.isEmpty()) {
            fieldsWithConsensus++;
        }
        if (!consensusAmounts.isEmpty()) {
            fieldsWithConsensus++;
        }
        if (!consensusCompanies.isEmpty()) {
            fieldsWithConsensus++;
        }

        return new ConsensusResult(
                invoiceId,
                consensusInvoiceNo,
                consensusDates,
                consensusAmounts,
                consensusCompanies,
                modelsAgreed,
                fieldsWithConsensus
        );
    }

    private String computeInvoiceNoConsensus(Iterable<ExtractionRun> modelRuns) {
        Map<String, Integer> counts = new HashMap<>();
        for (ExtractionRun run : modelRuns) {
            String raw = extractFieldAsString(run.getExtractedJson(), "INVOICE_NO");
            String normalized = normalizeInvoiceNo(raw);
            if (normalized == null) {
                continue;
            }
            counts.merge(normalized, 1, Integer::sum);
        }

        int max = 0;
        List<String> winners = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            int c = entry.getValue();
            if (c > max) {
                max = c;
                winners.clear();
                winners.add(entry.getKey());
            } else if (c == max) {
                winners.add(entry.getKey());
            }
        }

        if (max >= 2 && winners.size() == 1) {
            return winners.get(0);
        }
        return null;
    }

    private Set<String> computeSetConsensus(
            Iterable<ExtractionRun> modelRuns,
            String field,
            java.util.function.Function<String, String> normalizer
    ) {
        Map<String, Integer> valueModelCounts = new HashMap<>();

        for (ExtractionRun run : modelRuns) {
            Set<String> normalizedFromModel = new HashSet<>();
            for (String value : extractFieldAsSet(run.getExtractedJson(), field)) {
                String normalized = normalizer.apply(value);
                if (normalized == null || normalized.isBlank()) {
                    continue;
                }
                normalizedFromModel.add(normalized);
            }
            for (String normalized : normalizedFromModel) {
                valueModelCounts.merge(normalized, 1, Integer::sum);
            }
        }

        List<String> consensus = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : valueModelCounts.entrySet()) {
            if (entry.getValue() >= 3) {
                consensus.add(entry.getKey());
            }
        }
        Collections.sort(consensus);
        return new LinkedHashSet<>(consensus);
    }

    private Set<String> computeCompanyConsensus(Iterable<ExtractionRun> modelRuns) {
        Map<String, Integer> valueModelCounts = new HashMap<>();
        Map<String, String> displayByKey = new HashMap<>();

        for (ExtractionRun run : modelRuns) {
            Set<String> keysFromModel = new HashSet<>();
            for (String value : extractFieldAsSet(run.getExtractedJson(), "COMPANY")) {
                String key = normalizeCompanyKey(value);
                if (key == null || key.isBlank()) {
                    continue;
                }
                keysFromModel.add(key);
                displayByKey.putIfAbsent(key, toTitleCase(key));
            }
            for (String key : keysFromModel) {
                valueModelCounts.merge(key, 1, Integer::sum);
            }
        }

        List<String> consensus = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : valueModelCounts.entrySet()) {
            if (entry.getValue() >= 3) {
                consensus.add(displayByKey.getOrDefault(entry.getKey(), toTitleCase(entry.getKey())));
            }
        }
        Collections.sort(consensus);
        return new LinkedHashSet<>(consensus);
    }

    private String normalizeInvoiceNo(String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.trim().toUpperCase(Locale.ROOT).replaceAll("\\s+", "");
        return normalized.isBlank() ? null : normalized;
    }

    private String normalizeDateValue(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.isBlank()) {
            return null;
        }
        for (DateTimeFormatter formatter : DATE_INPUT_FORMATS) {
            try {
                LocalDate parsed = LocalDate.parse(trimmed, formatter);
                return parsed.format(DATE_OUTPUT);
            } catch (DateTimeParseException ignored) {
                // Try next format.
            }
        }
        return trimmed;
    }

    private String normalizeAmountValue(String value) {
        if (value == null) {
            return null;
        }
        String cleaned = value.trim().replace(",", "");
        if (cleaned.isBlank()) {
            return null;
        }
        try {
            BigDecimal amount = new BigDecimal(cleaned).setScale(2, RoundingMode.HALF_UP);
            return amount.toPlainString();
        } catch (NumberFormatException ex) {
            return cleaned;
        }
    }

    private String normalizeCompanyKey(String value) {
        if (value == null) {
            return null;
        }
        String collapsed = value.trim().replaceAll("\\s+", " ");
        if (collapsed.isBlank()) {
            return null;
        }
        return collapsed.toLowerCase(Locale.ROOT);
    }

    private String toTitleCase(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return Arrays.stream(value.trim().split("\\s+"))
                .filter(s -> !s.isBlank())
                .map(s -> s.substring(0, 1).toUpperCase(Locale.ROOT) + s.substring(1).toLowerCase(Locale.ROOT))
                .reduce((a, b) -> a + " " + b)
                .orElse("");
    }

    private String canonicalModel(String modelName) {
        if (modelName == null) {
            return null;
        }
        String v = modelName.trim().toLowerCase(Locale.ROOT);
        if (v.contains("gemini")) {
            return "gemini";
        }
        if (v.contains("llama")) {
            return "llama";
        }
        if (v.contains("mistral")) {
            return "mistral";
        }
        if (v.equals("regex")) {
            return "regex";
        }
        return null;
    }

    private Set<String> extractFieldAsSet(String json, String field) {
        if (json == null || json.isBlank()) {
            return Collections.emptySet();
        }
        try {
            JsonNode root = objectMapper.readTree(json);
            JsonNode node = root.path(field);
            if (node.isMissingNode() || node.isNull()) {
                return Collections.emptySet();
            }
            Set<String> out = new LinkedHashSet<>();
            if (node.isArray()) {
                for (JsonNode item : node) {
                    String val = item.asText("");
                    if (!val.isBlank()) {
                        out.add(val.trim());
                    }
                }
            } else {
                String val = node.asText("");
                if (!val.isBlank()) {
                    out.add(val.trim());
                }
            }
            return out;
        } catch (Exception ex) {
            log.debug("Failed to parse JSON set field {}: {}", field, ex.getMessage());
            return Collections.emptySet();
        }
    }

    private String extractFieldAsString(String json, String field) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            JsonNode root = objectMapper.readTree(json);
            JsonNode node = root.path(field);
            if (node.isMissingNode() || node.isNull()) {
                return null;
            }
            String val = node.asText(null);
            return val == null ? null : val.trim();
        } catch (Exception ex) {
            log.debug("Failed to parse JSON string field {}: {}", field, ex.getMessage());
            return null;
        }
    }

    public record ConsensusResult(
            String invoiceId,
            String invoiceNo,
            Set<String> dates,
            Set<String> amounts,
            Set<String> companies,
            int modelsAgreed,
            int fieldsWithConsensus
    ) {
    }
}
