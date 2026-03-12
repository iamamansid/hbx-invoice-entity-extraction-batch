// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/extractor/RegexExtractor.java
package com.research.hbx_invoice_entity_extraction_batch.batch.service.extractor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.ExtractionRunResult;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.NormalizationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Component
@Order(91)
@ConditionalOnProperty(name = "regex.extractor.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
public class RegexExtractor implements Extractor {

    private final ObjectMapper objectMapper;
    private final NormalizationService normalizationService;

    private static final Pattern DATE_PATTERN = Pattern.compile("\\b(?:(\\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01])|(0?[1-9]|[12]\\d|3[01])[.\\/-](0?[1-9]|1[0-2])[.\\/-]((?:19|20)\\d{2}))\\b");
    private static final Pattern LABELED_AMOUNT_PATTERN = Pattern.compile(
            "(?i)(?:total|amount\\s*due|balance\\s*due|grand\\s*total|subtotal|sub\\s*total|tax|vat|gst|due|payment\\s*due|invoice\\s*total|net\\s*total)[^\\n\\r]{0,60}?(\\d{1,3}(?:,\\d{3})*(?:\\.\\d{2}))",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern TABLE_AMOUNT_PATTERN = Pattern.compile(
            "(?m)^.*?(?:\\t|  {2,}).*?(\\d{1,3}(?:,\\d{3})*\\.\\d{2})\\s*$"
    );
    private static final Pattern RAW_AMOUNT_TOKEN_PATTERN = Pattern.compile("\\d{1,3}(?:,\\d{3})*(?:\\.\\d{2})");
    private static final Pattern INVOICE_NO_PATTERN = Pattern.compile("(?i)(invoice\\s*(number|no|#)[:\\s]*[#]?[A-Z0-9\\-]+)");
    private static final Pattern COMPANY_PATTERN = Pattern.compile(
            "(?i)(?:client\\s+)?(?:company|vendor|supplier|seller|from|bill\\s*from|issued\\s*by|bank\\s*name|account\\s*name)[:\\s]+([A-Z][a-zA-Z0-9\\s&.,'-]{2,60})"
    );
    private static final Pattern DATE_KEYWORD_PATTERN = Pattern.compile("(?i)\\b(?:date|invoice\\s*date|due\\s*date|dated)\\b");
    private static final Pattern ACCOUNT_OR_ROUTING_NEAR_PATTERN = Pattern.compile("(?i)(?:routing|account\\s*(?:number|no))[^\\n\\r]{0,20}$");
    private static final Pattern QUANTITY_LINE_PATTERN = Pattern.compile("(?i)\\b(?:qty|quantity|pcs|units|ea)\\b");
    private static final Pattern ADDRESS_LIKE_COMPANY_PATTERN = Pattern.compile("(?i).*\\d+\\s+.*\\b(?:road|street|ave|avenue|blvd|boulevard)\\b.*");
    private static final BigDecimal DATE_LIKE_MAX = new BigDecimal("31.12");
    private static final BigDecimal SMALL_QUANTITY_MAX = new BigDecimal("12.00");

    @Override
    public ExtractionRunResult extract(String invoiceId, String text, int runNumber) {
        long start = System.currentTimeMillis();
        String sourceText = text == null ? "" : text;

        try {
            ObjectNode root = objectMapper.createObjectNode();

            Matcher invMatcher = INVOICE_NO_PATTERN.matcher(sourceText);
            if (invMatcher.find()) {
                String fullMatch = invMatcher.group(0);
                String invStr = fullMatch.replaceAll("(?i)(invoice\\s*(number|no|#)[:\\s]*[#]?)", "").trim();
                root.put("INVOICE_NO", invStr);
            } else {
                root.putNull("INVOICE_NO");
            }

            ArrayNode dateArray = root.putArray("DATE");
            for (String normalizedDate : extractDates(sourceText)) {
                dateArray.add(normalizedDate);
            }

            ArrayNode amountArray = root.putArray("AMOUNT");
            for (String normalizedAmount : extractAmounts(sourceText)) {
                amountArray.add(normalizedAmount);
            }

            ArrayNode companyArray = root.putArray("COMPANY");
            for (String normalizedCompany : extractCompanies(sourceText)) {
                companyArray.add(normalizedCompany);
            }

            long latency = System.currentTimeMillis() - start;

            return ExtractionRunResult.builder()
                    .invoiceId(invoiceId)
                    .modelName(getModelName())
                    .runNumber(runNumber)
                    .extractedJson(objectMapper.writeValueAsString(root))
                    .latencyMs((int) latency)
                    .tokenCount(null)
                    .extractionStatus("COMPLETED")
                    .errorMessage(null)
                    .build();

        } catch (Exception e) {
            log.debug("Could not parse: {}", sourceText);
            log.error("Regex extraction failed for ID {}", invoiceId, e);
            return ExtractionRunResult.builder()
                    .invoiceId(invoiceId)
                    .modelName(getModelName())
                    .runNumber(runNumber)
                    .extractionStatus("FAILED")
                    .errorMessage(e.getMessage())
                    .build();
        }
    }

    private Set<String> extractDates(String text) {
        Set<String> dates = new LinkedHashSet<>();
        Matcher dateMatcher = DATE_PATTERN.matcher(text);
        while (dateMatcher.find()) {
            String raw = dateMatcher.group(0);
            String normalized = normalizationService.normalizeDate(raw);
            if (normalized != null && !normalized.isBlank()) {
                dates.add(normalized);
            }
        }
        return dates;
    }

    private Set<String> extractAmounts(String text) {
        Set<String> labeled = extractLabeledAmounts(text);
        Set<String> combined = new LinkedHashSet<>(labeled);
        for (String tableAmount : extractTableAmounts(text)) {
            if (!combined.contains(tableAmount)) {
                combined.add(tableAmount);
            }
        }
        return applyExclusionRules(combined, text);
    }

    private Set<String> extractLabeledAmounts(String text) {
        Set<String> amounts = new LinkedHashSet<>();
        Matcher matcher = LABELED_AMOUNT_PATTERN.matcher(text);
        while (matcher.find()) {
            String raw = matcher.group(1);
            String normalized = normalizationService.normalizeAmount(raw);
            if (normalized != null) {
                amounts.add(normalized);
            }
        }
        return amounts;
    }

    private Set<String> extractTableAmounts(String text) {
        Set<String> amounts = new LinkedHashSet<>();
        Matcher matcher = TABLE_AMOUNT_PATTERN.matcher(text);
        while (matcher.find()) {
            String raw = matcher.group(1);
            String normalized = normalizationService.normalizeAmount(raw);
            if (normalized != null) {
                amounts.add(normalized);
            }
        }
        return amounts;
    }

    private Set<String> applyExclusionRules(Set<String> amounts, String text) {
        Set<String> filtered = new LinkedHashSet<>();
        Set<String> normalizedDates = extractDates(text);

        for (String amount : amounts) {
            if (amount == null || amount.isBlank()) {
                continue;
            }
            if (normalizedDates.contains(amount)) {
                continue;
            }

            BigDecimal amountValue = parseAmount(amount);
            if (amountValue == null) {
                continue;
            }

            Matcher tokenMatcher = RAW_AMOUNT_TOKEN_PATTERN.matcher(text);
            boolean matchedInText = false;
            boolean validOccurrence = false;
            boolean invalidOnly = false;

            while (tokenMatcher.find()) {
                String rawToken = tokenMatcher.group();
                String normalizedToken = normalizationService.normalizeAmount(rawToken);
                if (normalizedToken == null || !amount.equals(normalizedToken)) {
                    continue;
                }

                matchedInText = true;
                boolean invalidOccurrence = false;
                String line = extractLine(text, tokenMatcher.start(), tokenMatcher.end());

                if (isDateLikeFalsePositive(amountValue, text, tokenMatcher.start(), tokenMatcher.end())) {
                    invalidOccurrence = true;
                }
                if (isNearAccountOrRoutingKeyword(text, tokenMatcher.start())) {
                    invalidOccurrence = true;
                }
                if (isQuantityLikeValue(amountValue, line)) {
                    invalidOccurrence = true;
                }

                if (invalidOccurrence) {
                    invalidOnly = true;
                } else {
                    validOccurrence = true;
                }
            }

            if (!matchedInText || validOccurrence || !invalidOnly) {
                filtered.add(amount);
            }
        }

        return filtered;
    }

    private Set<String> extractCompanies(String text) {
        Set<String> companies = new LinkedHashSet<>();
        Matcher companyMatcher = COMPANY_PATTERN.matcher(text);
        while (companyMatcher.find()) {
            String raw = companyMatcher.group(1);
            if (raw == null) {
                continue;
            }
            String trimmed = raw.trim();
            if (trimmed.length() < 3 || trimmed.length() > 80) {
                continue;
            }
            if (ADDRESS_LIKE_COMPANY_PATTERN.matcher(trimmed).matches()) {
                continue;
            }
            String normalized = normalizationService.normalizeCompany(trimmed);
            if (normalized != null && !normalized.isBlank()) {
                companies.add(normalized);
            }
        }
        return companies;
    }

    private BigDecimal parseAmount(String raw) {
        try {
            return new BigDecimal(raw);
        } catch (NumberFormatException ex) {
            log.debug("Could not parse: {}", raw);
            return null;
        }
    }

    private boolean isDateLikeFalsePositive(BigDecimal value, String text, int start, int end) {
        if (value.compareTo(DATE_LIKE_MAX) > 0) {
            return false;
        }
        int left = Math.max(0, start - 80);
        int right = Math.min(text.length(), end + 80);
        String window = text.substring(left, right);
        return DATE_KEYWORD_PATTERN.matcher(window).find();
    }

    private boolean isNearAccountOrRoutingKeyword(String text, int amountStart) {
        int left = Math.max(0, amountStart - 30);
        String prefix = text.substring(left, amountStart);
        return ACCOUNT_OR_ROUTING_NEAR_PATTERN.matcher(prefix).find();
    }

    private boolean isQuantityLikeValue(BigDecimal value, String line) {
        if (value.compareTo(SMALL_QUANTITY_MAX) > 0) {
            return false;
        }
        return QUANTITY_LINE_PATTERN.matcher(line).find();
    }

    private String extractLine(String text, int start, int end) {
        int lineStart = text.lastIndexOf('\n', Math.max(0, start - 1));
        int lineEnd = text.indexOf('\n', end);
        if (lineStart < 0) {
            lineStart = 0;
        } else {
            lineStart = lineStart + 1;
        }
        if (lineEnd < 0) {
            lineEnd = text.length();
        }
        return text.substring(lineStart, lineEnd);
    }

    @Override
    public String getModelName() {
        return "regex-v2";
    }
}
