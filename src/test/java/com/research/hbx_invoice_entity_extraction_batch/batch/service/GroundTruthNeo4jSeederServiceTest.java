package com.research.hbx_invoice_entity_extraction_batch.batch.service;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.GroundTruthConsensus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionCallback;
import org.neo4j.driver.TransactionContext;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GroundTruthNeo4jSeederServiceTest {

    @Mock
    private Driver neo4jDriver;

    @Mock
    private Session session;

    @Mock
    private TransactionContext tx;

    @Mock
    private Result result;

    @Mock
    private NormalizationService normalizationService;

    private GroundTruthNeo4jSeederService service;

    @BeforeEach
    void setUp() {
        service = new GroundTruthNeo4jSeederService(neo4jDriver, normalizationService);

        when(neo4jDriver.session()).thenReturn(session);
        when(session.executeWrite(any(TransactionCallback.class))).thenAnswer(invocation -> {
            TransactionCallback<?> callback = invocation.getArgument(0);
            return callback.execute(tx);
        });
        when(tx.run(anyString(), anyMap())).thenReturn(result);
    }

    @Test
    void seedsNormalizedValuesAndFallsBackToTrimmedRawValues() {
        GroundTruthConsensus gt = GroundTruthConsensus.builder()
                .invoiceId("INV-001")
                .consensusDates(" 03/15/2024 , bad-date ")
                .consensusAmounts(" $12.5 , not-an-amount ")
                .consensusCompanies(" Acme Corp , Beta LLC ")
                .build();

        when(normalizationService.normalizeDate("03/15/2024")).thenReturn("2024-03-15");
        when(normalizationService.normalizeDate("bad-date")).thenReturn(" ");
        when(normalizationService.normalizeAmount("$12.5")).thenReturn("12.50");
        when(normalizationService.normalizeAmount("not-an-amount")).thenReturn(null);

        service.seedGroundTruth(gt);

        verify(tx).run(
                argThat(query -> query.contains("MERGE (d:DateNode")),
                argThat((Map<String, Object> params) ->
                        "INV-001".equals(params.get("invoiceNo")) && "2024-03-15".equals(params.get("value"))));
        verify(tx).run(
                argThat(query -> query.contains("MERGE (d:DateNode")),
                argThat((Map<String, Object> params) ->
                        "INV-001".equals(params.get("invoiceNo")) && "bad-date".equals(params.get("value"))));
        verify(tx).run(
                argThat(query -> query.contains("MERGE (a:AmountNode")),
                argThat((Map<String, Object> params) ->
                        "INV-001".equals(params.get("invoiceNo")) && "12.50".equals(params.get("value"))));
        verify(tx).run(
                argThat(query -> query.contains("MERGE (a:AmountNode")),
                argThat((Map<String, Object> params) ->
                        "INV-001".equals(params.get("invoiceNo")) && "not-an-amount".equals(params.get("value"))));
        verify(tx).run(
                argThat(query -> query.contains("MERGE (c:CompanyNode")),
                argThat((Map<String, Object> params) ->
                        "INV-001".equals(params.get("invoiceNo")) && "Acme Corp".equals(params.get("value"))));
        verify(tx).run(
                argThat(query -> query.contains("MERGE (c:CompanyNode")),
                argThat((Map<String, Object> params) ->
                        "INV-001".equals(params.get("invoiceNo")) && "Beta LLC".equals(params.get("value"))));
        verify(tx, times(8)).run(anyString(), anyMap());

        verify(normalizationService).normalizeDate("03/15/2024");
        verify(normalizationService).normalizeDate("bad-date");
        verify(normalizationService).normalizeAmount("$12.5");
        verify(normalizationService).normalizeAmount("not-an-amount");
        verifyNoMoreInteractions(normalizationService);
    }
}
