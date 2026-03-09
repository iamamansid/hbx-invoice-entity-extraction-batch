// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/repository/GroundTruthConsensusRepository.java
package com.research.hbx_invoice_entity_extraction_batch.batch.repository;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.GroundTruthConsensus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface GroundTruthConsensusRepository extends JpaRepository<GroundTruthConsensus, String> {

    List<GroundTruthConsensus> findBySeededToNeo4jFalse();

    @Query("SELECT COUNT(g) FROM GroundTruthConsensus g WHERE g.seededToNeo4j = true")
    long countSeeded();

    @Query("SELECT COUNT(g) FROM GroundTruthConsensus g WHERE g.seededToNeo4j = false")
    long countPending();

    @Query("SELECT COUNT(g) FROM GroundTruthConsensus g")
    long countTotalConsensusReached();

    @Query("SELECT AVG(g.fieldsWithConsensus) FROM GroundTruthConsensus g")
    Double averageFieldsWithConsensus();
}
