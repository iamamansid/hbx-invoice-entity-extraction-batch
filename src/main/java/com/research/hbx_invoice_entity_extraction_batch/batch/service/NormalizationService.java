// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/service/NormalizationService.java
package com.research.hbx_invoice_entity_extraction_batch.batch.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Service
public class NormalizationService {

    private static final Pattern DATE_DOT_PATTERN = Pattern.compile("\\b\\d{1,2}\\.\\d{1,2}\\.\\d{4}\\b");
    private static final Pattern DATE_SLASH_PATTERN = Pattern.compile("\\b\\d{1,2}/\\d{1,2}/\\d{4}\\b");
    private static final Pattern DATE_DASH_PATTERN = Pattern.compile("\\b\\d{1,2}-\\d{1,2}-\\d{4}\\b");
    private static final Pattern AMOUNT_PATTERN = Pattern.compile("\\b\\d{1,3}(,\\d{3})*(\\.\\d{1,2})?\\b");
    private static final DateTimeFormatter YYYY_MM_DD = DateTimeFormatter.ofPattern("uuuu-MM-dd").withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter DD_DOT_MM_YYYY = DateTimeFormatter.ofPattern("dd.MM.uuuu").withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter DD_SLASH_MM_YYYY = DateTimeFormatter.ofPattern("dd/MM/uuuu").withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter MM_SLASH_DD_YYYY = DateTimeFormatter.ofPattern("MM/dd/uuuu").withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter DD_DASH_MM_YYYY = DateTimeFormatter.ofPattern("dd-MM-uuuu").withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter MM_DASH_DD_YYYY = DateTimeFormatter.ofPattern("MM-dd-uuuu").withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter D_DOT_M_YYYY = DateTimeFormatter.ofPattern("d.M.uuuu").withResolverStyle(ResolverStyle.STRICT);
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
        if (value.isEmpty()) {
            return value;
        }

        LocalDate parsed = parseDate(value, YYYY_MM_DD);
        if (parsed != null && isValidDate(parsed)) {
            return parsed.format(ISO_DATE);
        }

        parsed = parseDate(value, DD_DOT_MM_YYYY);
        if (parsed != null && isValidDate(parsed)) {
            return parsed.format(ISO_DATE);
        }

        parsed = parseDate(value, DD_SLASH_MM_YYYY);
        if (parsed != null && isValidDate(parsed)) {
            return parsed.format(ISO_DATE);
        }

        parsed = parseDate(value, MM_SLASH_DD_YYYY);
        if (parsed != null && isValidDate(parsed)) {
            return parsed.format(ISO_DATE);
        }

        parsed = parseDate(value, DD_DASH_MM_YYYY);
        if (parsed != null && isValidDate(parsed)) {
            return parsed.format(ISO_DATE);
        }

        parsed = parseDate(value, MM_DASH_DD_YYYY);
        if (parsed != null && isValidDate(parsed)) {
            return parsed.format(ISO_DATE);
        }

        parsed = parseDate(value, D_DOT_M_YYYY);
        if (parsed != null && isValidDate(parsed)) {
            return parsed.format(ISO_DATE);
        }

        return value;
    }

    public String normalizeAmount(String rawAmount) {
        if (rawAmount == null) {
            return null;
        }
        String cleaned = rawAmount.trim();
        if (cleaned.isEmpty()) {
            return null;
        }

        cleaned = cleaned.replaceAll("(?i)\\b(?:USD|EUR)\\b", "");
        cleaned = cleaned.replaceAll("[$\\u00A3\\u20AC\\u20B9]", "");
        cleaned = cleaned.replace(",", "").trim();

        if (cleaned.isEmpty()) {
            return null;
        }
        if (!cleaned.matches("\\d+(?:\\.\\d+)?")) {
            return null;
        }

        try {
            BigDecimal value = new BigDecimal(cleaned).setScale(2, RoundingMode.HALF_UP);
            if (value.compareTo(BigDecimal.ZERO) <= 0) {
                return null;
            }
            return value.toPlainString();
        } catch (NumberFormatException ex) {
            log.debug("Could not parse: {}", rawAmount);
            return null;
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
            String raw = matcher.group();
            String normalized = normalizeAmount(raw);
            matcher.appendReplacement(output, Matcher.quoteReplacement(normalized == null ? raw : normalized));
        }
        matcher.appendTail(output);
        return output.toString();
    }

    private String normalizeDateWithFormatter(String value, DateTimeFormatter formatter) {
        LocalDate parsed = parseDate(value, formatter);
        if (parsed == null || !isValidDate(parsed)) {
            return value;
        }
        return parsed.format(ISO_DATE);
    }

    private LocalDate parseDate(String value, DateTimeFormatter formatter) {
        try {
            return LocalDate.parse(value, formatter);
        } catch (DateTimeParseException ex) {
            log.debug("Could not parse: {}", value);
            return null;
        }
    }

    private boolean isValidDate(LocalDate date) {
        if (date == null) {
            return false;
        }
        long year = date.getLong(ChronoField.YEAR);
        long month = date.getLong(ChronoField.MONTH_OF_YEAR);
        long day = date.getLong(ChronoField.DAY_OF_MONTH);

        if (!ChronoField.YEAR.range().isValidValue(year)
                || !ChronoField.MONTH_OF_YEAR.range().isValidValue(month)
                || !ChronoField.DAY_OF_MONTH.range().isValidValue(day)) {
            return false;
        }
        return year >= 2000 && year <= 2030;
    }
}
