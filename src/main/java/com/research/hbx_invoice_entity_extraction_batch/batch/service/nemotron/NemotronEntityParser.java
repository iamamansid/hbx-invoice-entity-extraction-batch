// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/nemotron/NemotronEntityParser.java
package com.research.hbx_invoice_entity_extraction_batch.batch.service.nemotron;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.nemotron.NemotronOcrResponse;
import com.research.hbx_invoice_entity_extraction_batch.batch.model.dto.nemotron.TextDetection;
import com.research.hbx_invoice_entity_extraction_batch.batch.service.NormalizationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@Slf4j
@RequiredArgsConstructor
public class NemotronEntityParser {

    private static final double MIN_CONFIDENCE = 0.65;
    private static final double MIN_DATE_CONFIDENCE = 0.70;
    private static final BigDecimal SMALL_QUANTITY_MAX = new BigDecimal("12.00");
    private static final Pattern DAY_FIRST_DATE_PATTERN = Pattern.compile(
            "\\b(0?[1-9]|[12]\\d|3[01])[./-](0?[1-9]|1[0-2])[./-](19|20)\\d{2}\\b"
    );
    private static final Pattern ISO_DATE_PATTERN = Pattern.compile(
            "\\b\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01])\\b"
    );
    private static final Pattern AMOUNT_VALUE_PATTERN = Pattern.compile(
            "(?i)(?:\\$\\s*|(?:USD|EUR|GBP|INR)\\s*)?\\d{1,3}(?:,\\d{3})*(?:\\.\\d{2})"
    );
    private static final Pattern EXCLUDED_AMOUNT_CONTEXT_PATTERN = Pattern.compile(
            "(?i)(routing\\s+number:|account\\s+number:|account\\s+no:|phone:|tel:)"
    );
    private static final Pattern QUANTITY_PATTERN = Pattern.compile("(?i)\\b(qty|quantity|pcs|units|ea)\\b");

    private static final List<String> INVOICE_PREFIXES = List.of(
            "invoice number:", "invoice no:", "invoice #:", "invoice no."
    );
    private static final List<String> DATE_PREFIXES = List.of(
            "invoice date:", "date:", "due date:", "payment date:", "issue date:"
    );
    private static final List<String> AMOUNT_PREFIXES = List.of(
            "total:", "amount due:", "balance due:", "grand total:",
            "subtotal:", "invoice total:", "amount:", "total amount:"
    );
    private static final List<String> COMPANY_PREFIXES = List.of(
            "client company:", "company:", "bank name:", "account name:",
            "vendor:", "supplier:", "seller:", "from:", "bill from:",
            "issued by:", "service provider:"
    );

    private final NormalizationService normalizationService;

    public ParsedEntities parse(NemotronOcrResponse response) {
        try {
            List<TextDetection> sorted = response == null
                    ? List.of()
                    : response.sortedByReadingOrder().stream()
                    .filter(detection -> detection != null && detection.getConfidence() >= MIN_CONFIDENCE)
                    .toList();

            return new ParsedEntities(
                    extractInvoiceNo(sorted),
                    extractDates(sorted),
                    extractAmounts(sorted),
                    extractCompanies(sorted)
            );
        } catch (Exception e) {
            log.error("Failed to parse Nemotron OCR detections", e);
            return new ParsedEntities(null, List.of(), List.of(), List.of());
        }
    }

    public record ParsedEntities(
            String invoiceNo,
            List<String> dates,
            List<String> amounts,
            List<String> companies
    ) {
    }

    private String extractInvoiceNo(List<TextDetection> sorted) {
        for (TextDetection detection : sorted) {
            String value = extractAfterPrefix(safeText(detection), INVOICE_PREFIXES);
            if (value == null) {
                continue;
            }

            String normalized = value.replaceFirst("^[#\\s]+", "").trim();
            if (!normalized.isBlank()) {
                return normalized.toUpperCase(Locale.ROOT);
            }
        }
        return null;
    }

    private List<String> extractDates(List<TextDetection> sorted) {
        LinkedHashSet<String> dates = new LinkedHashSet<>();

        for (TextDetection detection : sorted) {
            if (detection.getConfidence() < MIN_DATE_CONFIDENCE) {
                continue;
            }

            String text = safeText(detection);
            String labeledValue = extractAfterPrefix(text, DATE_PREFIXES);
            if (labeledValue != null) {
                addDateCandidate(dates, labeledValue);
            }

            if (matchesDatePattern(text)) {
                addNormalizedDate(dates, text);
            }
        }

        return new ArrayList<>(dates);
    }

    private List<String> extractAmounts(List<TextDetection> sorted) {
        LinkedHashSet<String> amounts = new LinkedHashSet<>();
        List<AmountCandidate> candidates = new ArrayList<>();

        for (TextDetection detection : sorted) {
            String labeledValue = extractAfterPrefix(safeText(detection), AMOUNT_PREFIXES);
            if (labeledValue == null) {
                continue;
            }

            Matcher matcher = AMOUNT_VALUE_PATTERN.matcher(labeledValue);
            if (matcher.find()) {
                candidates.add(new AmountCandidate(matcher.group(), detection));
            }
        }

        TextDetection header = findAmountHeader(sorted);
        if (header != null) {
            double columnXMin = header.getXMin() - 0.02;
            double columnXMax = header.getXMax() + 0.02;
            double headerY = header.getYMin();

            for (TextDetection detection : sorted) {
                if (detection == header) {
                    continue;
                }
                if (detection.getXMin() >= columnXMin
                        && detection.getXMax() <= columnXMax + 0.05
                        && detection.isBelowY(headerY)
                        && detection.looksLikeAmount()) {
                    candidates.add(new AmountCandidate(safeText(detection), detection));
                }
            }
        }

        for (AmountCandidate candidate : candidates) {
            if (shouldExcludeAmount(candidate)) {
                continue;
            }

            String normalized = normalizationService.normalizeAmount(candidate.rawValue());
            if (normalized != null && !normalized.isBlank()) {
                amounts.add(normalized);
            }
        }

        return new ArrayList<>(amounts);
    }

    private List<String> extractCompanies(List<TextDetection> sorted) {
        LinkedHashSet<String> companies = new LinkedHashSet<>();

        for (TextDetection detection : sorted) {
            String labeledValue = extractAfterPrefix(safeText(detection), COMPANY_PREFIXES);
            if (labeledValue == null) {
                continue;
            }

            String normalized = normalizationService.normalizeCompany(labeledValue);
            if (normalized == null || normalized.isBlank()) {
                continue;
            }
            if (normalized.length() < 3 || normalized.length() > 80) {
                continue;
            }
            if (normalized.matches(".*\\d+\\s+[A-Za-z]+.*")) {
                continue;
            }
            companies.add(normalized);
        }

        return new ArrayList<>(companies);
    }

    private String extractAfterPrefix(String text, List<String> prefixes) {
        if (text == null) {
            return null;
        }

        String trimmed = text.trim();
        String lower = trimmed.toLowerCase(Locale.ROOT);
        for (String prefix : prefixes) {
            if (lower.startsWith(prefix.toLowerCase(Locale.ROOT))) {
                return trimmed.substring(prefix.length()).trim();
            }
        }
        return null;
    }

    private void addDateCandidate(LinkedHashSet<String> dates, String rawText) {
        String candidate = findFirstDate(rawText);
        if (candidate != null) {
            addNormalizedDate(dates, candidate);
        }
    }

    private void addNormalizedDate(LinkedHashSet<String> dates, String rawDate) {
        String normalized = normalizationService.normalizeDate(rawDate);
        if (normalized != null && !normalized.isBlank()) {
            dates.add(normalized);
        }
    }

    private String findFirstDate(String text) {
        Matcher dayFirstMatcher = DAY_FIRST_DATE_PATTERN.matcher(text);
        if (dayFirstMatcher.find()) {
            return dayFirstMatcher.group();
        }

        Matcher isoMatcher = ISO_DATE_PATTERN.matcher(text);
        if (isoMatcher.find()) {
            return isoMatcher.group();
        }

        return null;
    }

    private boolean matchesDatePattern(String text) {
        return DAY_FIRST_DATE_PATTERN.matcher(text).matches() || ISO_DATE_PATTERN.matcher(text).matches();
    }

    private TextDetection findAmountHeader(List<TextDetection> sorted) {
        for (TextDetection detection : sorted) {
            String text = safeText(detection);
            if ("amount".equalsIgnoreCase(text) || "total".equalsIgnoreCase(text)) {
                return detection;
            }
        }
        return null;
    }

    private boolean shouldExcludeAmount(AmountCandidate candidate) {
        String detectionText = safeText(candidate.detection());
        String lowerDetectionText = detectionText.toLowerCase(Locale.ROOT);
        String lowerRawValue = candidate.rawValue().toLowerCase(Locale.ROOT);
        int valueIndex = lowerDetectionText.indexOf(lowerRawValue);
        if (valueIndex > 0) {
            String prefixText = detectionText.substring(0, valueIndex);
            if (EXCLUDED_AMOUNT_CONTEXT_PATTERN.matcher(prefixText).find()) {
                return true;
            }
        }

        String normalized = normalizationService.normalizeAmount(candidate.rawValue());
        if (normalized == null) {
            return true;
        }

        if (matchesDatePattern(candidate.rawValue())) {
            return true;
        }

        try {
            BigDecimal amountValue = new BigDecimal(normalized);
            if (amountValue.compareTo(SMALL_QUANTITY_MAX) <= 0 && QUANTITY_PATTERN.matcher(detectionText).find()) {
                return true;
            }
        } catch (NumberFormatException e) {
            return true;
        }

        return false;
    }

    private String safeText(TextDetection detection) {
        return detection.getText() == null ? "" : detection.getText().trim();
    }

    private record AmountCandidate(String rawValue, TextDetection detection) {
    }
}
