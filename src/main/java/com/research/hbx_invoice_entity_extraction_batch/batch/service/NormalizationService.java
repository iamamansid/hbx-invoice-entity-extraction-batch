package com.research.hbx_invoice_entity_extraction_batch.batch.service;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class NormalizationService {

    private static final Pattern DATE_DOT_PATTERN = Pattern.compile("\\b\\d{2}\\.\\d{2}\\.\\d{4}\\b");
    private static final Pattern DATE_SLASH_PATTERN = Pattern.compile("\\b\\d{2}/\\d{2}/\\d{4}\\b");
    private static final Pattern DATE_DASH_PATTERN = Pattern.compile("\\b\\d{2}-\\d{2}-\\d{4}\\b");
    private static final Pattern AMOUNT_PATTERN = Pattern.compile("\\b\\d{1,3}(,\\d{3})*(\\.\\d{1,2})?\\b");
    private static final DateTimeFormatter DD_DOT_MM_YYYY = DateTimeFormatter.ofPattern("dd.MM.yyyy");
    private static final DateTimeFormatter DD_SLASH_MM_YYYY = DateTimeFormatter.ofPattern("dd/MM/yyyy");
    private static final DateTimeFormatter MM_DASH_DD_YYYY = DateTimeFormatter.ofPattern("MM-dd-yyyy");
    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ISO_LOCAL_DATE;

    public String normalizeText(String rawText) {
        if (rawText == null) {
            return null;
        }
        String normalized = replaceDates(rawText, DATE_DOT_PATTERN, DD_DOT_MM_YYYY);
        normalized = replaceDates(normalized, DATE_SLASH_PATTERN, DD_SLASH_MM_YYYY);
        normalized = replaceDates(normalized, DATE_DASH_PATTERN, MM_DASH_DD_YYYY);
        normalized = replaceAmounts(normalized);
        normalized = normalized.replaceAll("[ \\t]+", " ").trim();
        return normalized;
    }

    public String normalizeDate(String rawDate) {
        if (rawDate == null) {
            return null;
        }
        String value = rawDate.trim();
        LocalDate parsed = parseDate(value, DD_DOT_MM_YYYY);
        if (parsed == null) {
            parsed = parseDate(value, DD_SLASH_MM_YYYY);
        }
        if (parsed == null) {
            parsed = parseDate(value, MM_DASH_DD_YYYY);
        }
        return parsed == null ? value : parsed.format(ISO_DATE);
    }

    public String normalizeAmount(String rawAmount) {
        if (rawAmount == null) {
            return null;
        }
        String cleaned = rawAmount.trim().replace(",", "");
        if (cleaned.isEmpty()) {
            return cleaned;
        }
        try {
            BigDecimal value = new BigDecimal(cleaned).setScale(2, RoundingMode.HALF_UP);
            return value.toPlainString();
        } catch (NumberFormatException ignored) {
            return cleaned;
        }
    }

    public String normalizeCompany(String company) {
        if (company == null) return null;
        return company.replaceAll("[ \\t]+", " ").trim();
    }

    private String replaceDates(String input, Pattern pattern, DateTimeFormatter formatter) {
        Matcher matcher = pattern.matcher(input);
        StringBuffer output = new StringBuffer();
        while (matcher.find()) {
            String normalized = normalizeDateWithFormatter(matcher.group(), formatter);
            matcher.appendReplacement(output, Matcher.quoteReplacement(normalized));
        }
        matcher.appendTail(output);
        return output.toString();
    }

    private String replaceAmounts(String input) {
        Matcher matcher = AMOUNT_PATTERN.matcher(input);
        StringBuffer output = new StringBuffer();
        while (matcher.find()) {
            String normalized = normalizeAmount(matcher.group());
            matcher.appendReplacement(output, Matcher.quoteReplacement(normalized));
        }
        matcher.appendTail(output);
        return output.toString();
    }

    private String normalizeDateWithFormatter(String value, DateTimeFormatter formatter) {
        LocalDate parsed = parseDate(value, formatter);
        return parsed == null ? value : parsed.format(ISO_DATE);
    }

    private LocalDate parseDate(String value, DateTimeFormatter formatter) {
        try {
            return LocalDate.parse(value, formatter);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }
}
