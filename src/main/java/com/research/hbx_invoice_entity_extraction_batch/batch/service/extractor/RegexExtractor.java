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

    private static final Pattern DATE_PATTERN = Pattern.compile("\\b(0?[1-9]|[12][0-9]|3[01])[./-](0?[1-9]|1[0-2])[./-](19|20)\\d{2}\\b");
    private static final Pattern AMOUNT_PATTERN = Pattern.compile("(?i)(?:total|amount|balance|subtotal|tax|due)[^\\n]{0,30}?\\b(\\d{1,3}(?:,\\d{3})*\\.\\d{2})\\b");
    private static final Pattern INVOICE_NO_PATTERN = Pattern.compile("(?i)(invoice\\s*(number|no|#)[:\\s]*[#]?[A-Z0-9\\-]+)");
    private static final Pattern COMPANY_PATTERN = Pattern.compile("(?:Client\\s+)?(?:Company|Bank Name|Account Name):\\s*([A-Z][a-zA-Z0-9\\s&.,]+)");

    // "COMPANY: keywords ["Company:", "Bank Name:", "Account Name:"] followed by capitalized words"
    
    @Override
    public ExtractionRunResult extract(String invoiceId, String text, int runNumber) {
        long start = System.currentTimeMillis();
        String sourceText = text == null ? "" : text;
        
        try {
            ObjectNode root = objectMapper.createObjectNode();
            
            // Extract INVOICE_NO
            Matcher invMatcher = INVOICE_NO_PATTERN.matcher(sourceText);
            if (invMatcher.find()) {
                String fullMatch = invMatcher.group(0);
                String invStr = fullMatch.replaceAll("(?i)(invoice\\s*(number|no|#)[:\\s]*[#]?)", "").trim();
                root.put("INVOICE_NO", invStr);
            } else {
                root.putNull("INVOICE_NO");
            }
            
            // Extract DATE
            ArrayNode dateArray = root.putArray("DATE");
            Matcher dateMatcher = DATE_PATTERN.matcher(sourceText);
            while (dateMatcher.find()) {
                String d = dateMatcher.group(0);
                dateArray.add(normalizationService.normalizeDate(d));
            }
            
            // Extract AMOUNT
            ArrayNode amountArray = root.putArray("AMOUNT");
            Matcher amountMatcher = AMOUNT_PATTERN.matcher(sourceText);
            while (amountMatcher.find()) {
                String a = amountMatcher.group(0);
                amountArray.add(normalizationService.normalizeAmount(a));
            }
            
            // Extract COMPANY
            ArrayNode companyArray = root.putArray("COMPANY");
            Matcher companyMatcher = COMPANY_PATTERN.matcher(sourceText);
            while (companyMatcher.find()) {
                String c = companyMatcher.group(1).trim();
                companyArray.add(normalizationService.normalizeCompany(c));
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

    @Override
    public String getModelName() {
        return "regex-v2";
    }
}
