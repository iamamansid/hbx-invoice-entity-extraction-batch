package com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.io.Serializable;
import java.time.LocalDateTime;

@Entity
@Table(name = "extraction_run")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@IdClass(ExtractionRun.ExtractionRunId.class)
public class ExtractionRun {

    @Id
    @Column(name = "invoice_id")
    private String invoiceId;

    @Id
    @Column(name = "model_name")
    private String modelName;

    @Id
    @Column(name = "run_number")
    private Integer runNumber;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "extracted_json", columnDefinition = "JSONB")
    private String extractedJson;

    @Column(name = "latency_ms")
    private Integer latencyMs;

    @Column(name = "token_count")
    private Integer tokenCount;

    @Column(name = "extraction_status")
    private String extractionStatus;

    @Column(name = "error_message", columnDefinition = "TEXT")
    private String errorMessage;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ExtractionRunId implements Serializable {
        private String invoiceId;
        private String modelName;
        private Integer runNumber;
    }
}
