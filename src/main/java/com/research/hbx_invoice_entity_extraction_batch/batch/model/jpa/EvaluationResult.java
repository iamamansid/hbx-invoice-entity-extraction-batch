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

import java.io.Serializable;
import java.time.LocalDateTime;

@Entity
@Table(name = "evaluation_result")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@IdClass(EvaluationResult.EvaluationResultId.class)
public class EvaluationResult {

    @Id
    @Column(name = "invoice_id")
    private String invoiceId;

    @Id
    @Column(name = "model_name")
    private String modelName;

    @Id
    @Column(name = "run_number")
    private Integer runNumber;

    @Column(name = "date_accuracy")
    private Double dateAccuracy;

    @Column(name = "date_f1")
    private Double dateF1;

    @Column(name = "amount_accuracy")
    private Double amountAccuracy;

    @Column(name = "amount_f1")
    private Double amountF1;

    @Column(name = "company_accuracy")
    private Double companyAccuracy;

    @Column(name = "company_f1")
    private Double companyF1;

    @Column(name = "overall_entity_accuracy")
    private Double overallEntityAccuracy;

    @Column(name = "relation_completeness")
    private Double relationCompleteness;

    @Column(name = "query_answerability")
    private Double queryAnswerability;

    @Column(name = "graph_idempotent")
    private Boolean graphIdempotent;

    @Column(name = "consistency_score")
    private Double consistencyScore;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class EvaluationResultId implements Serializable {
        private String invoiceId;
        private String modelName;
        private Integer runNumber;
    }
}
