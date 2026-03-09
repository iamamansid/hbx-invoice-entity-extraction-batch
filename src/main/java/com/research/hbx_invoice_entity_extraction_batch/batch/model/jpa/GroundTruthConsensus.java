// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/model/jpa/GroundTruthConsensus.java
package com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "ground_truth_consensus")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GroundTruthConsensus {

    @Id
    @Column(name = "invoice_id")
    private String invoiceId;

    @Column(name = "consensus_invoice_no")
    private String consensusInvoiceNo;

    @Column(name = "consensus_dates")
    private String consensusDates;

    @Column(name = "consensus_amounts")
    private String consensusAmounts;

    @Column(name = "consensus_companies")
    private String consensusCompanies;

    @Column(name = "models_agreed")
    private Integer modelsAgreed;

    @Column(name = "fields_with_consensus")
    private Integer fieldsWithConsensus;

    @Column(name = "seeded_to_neo4j")
    private Boolean seededToNeo4j;

    @Column(name = "created_at")
    private LocalDateTime createdAt;
}
