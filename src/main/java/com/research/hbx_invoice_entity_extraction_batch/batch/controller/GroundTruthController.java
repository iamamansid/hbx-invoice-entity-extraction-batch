// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/controller/GroundTruthController.java
package com.research.hbx_invoice_entity_extraction_batch.batch.controller;

import com.research.hbx_invoice_entity_extraction_batch.batch.repository.GroundTruthConsensusRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobExecutionException;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/admin/ground-truth")
@RequiredArgsConstructor
public class GroundTruthController {

    private final JobLauncher jobLauncher;

    @Qualifier("groundTruthConsensusJob")
    private final Job groundTruthConsensusJob;

    private final GroundTruthConsensusRepository groundTruthConsensusRepository;

    @PostMapping("/run")
    public ResponseEntity<Map<String, Object>> run() {
        JobParameters params = new JobParametersBuilder()
                .addLong("run.id", System.currentTimeMillis())
                .toJobParameters();
        try {
            JobExecution execution = jobLauncher.run(groundTruthConsensusJob, params);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("jobId", execution.getId());
            body.put("status", "STARTED");
            return ResponseEntity.ok(body);
        } catch (JobExecutionException e) {
            log.error("Failed to launch groundTruthConsensusJob", e);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("error", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
        }
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        long totalSeeded = groundTruthConsensusRepository.countSeeded();
        long totalPending = groundTruthConsensusRepository.countPending();
        long totalConsensusReached = groundTruthConsensusRepository.countTotalConsensusReached();
        Double avg = groundTruthConsensusRepository.averageFieldsWithConsensus();
        double averageFieldsWithConsensus = avg == null ? 0.0 : avg;

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("totalSeeded", totalSeeded);
        body.put("totalPending", totalPending);
        body.put("totalConsensusReached", totalConsensusReached);
        body.put("averageFieldsWithConsensus", averageFieldsWithConsensus);
        return ResponseEntity.ok(body);
    }
}
