package com.research.hbx_invoice_entity_extraction_batch.batch.web;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/admin/batch")
@RequiredArgsConstructor
public class BatchJobController {

    private final JobOperator jobOperator;

    @Qualifier("invoiceExtractionJob")
    private final Job invoiceExtractionJob;

    @Qualifier("ocrJob")
    private final Job ocrJob;

    @Qualifier("extractionJob")
    private final Job extractionJob;

    @Qualifier("evaluationJob")
    private final Job evaluationJob;

    @Qualifier("exportJob")
    private final Job exportJob;

    @PostMapping("/run/full")
    public ResponseEntity<JobLaunchResponse> runFull(@RequestParam(value = "runId", required = false) Long runId) {
        return launchJob(invoiceExtractionJob, runId);
    }

    @PostMapping("/run/ocr")
    public ResponseEntity<JobLaunchResponse> runOcr(@RequestParam(value = "runId", required = false) Long runId) {
        return launchJob(ocrJob, runId);
    }

    @PostMapping("/run/extraction")
    public ResponseEntity<JobLaunchResponse> runExtraction(@RequestParam(value = "runId", required = false) Long runId) {
        return launchJob(extractionJob, runId);
    }

    @PostMapping("/run/evaluation")
    public ResponseEntity<JobLaunchResponse> runEvaluation(@RequestParam(value = "runId", required = false) Long runId) {
        return launchJob(evaluationJob, runId);
    }

    @PostMapping("/run/export")
    public ResponseEntity<JobLaunchResponse> runExport(@RequestParam(value = "runId", required = false) Long runId) {
        return launchJob(exportJob, runId);
    }

    private ResponseEntity<JobLaunchResponse> launchJob(Job job, Long runId) {
        long effectiveRunId = runId != null ? runId : System.currentTimeMillis();
        JobParameters params = new JobParametersBuilder()
                .addLong("run.id", effectiveRunId)
                .toJobParameters();

        try {
            JobExecution execution = jobOperator.start(job, params);
            return ResponseEntity.ok(new JobLaunchResponse(
                    execution.getJobInstance().getJobName(),
                    execution.getId(),
                    effectiveRunId,
                    execution.getStatus().name(),
                    execution.getExitStatus().getExitCode(),
                    "Job launched"
            ));
        } catch (JobExecutionAlreadyRunningException | JobRestartException
                 | JobInstanceAlreadyCompleteException | InvalidJobParametersException e) {
            log.warn("Unable to launch job {}", job.getName(), e);
            return ResponseEntity.badRequest().body(new JobLaunchResponse(
                    job.getName(),
                    null,
                    effectiveRunId,
                    "FAILED_TO_LAUNCH",
                    "FAILED",
                    e.getMessage()
            ));
        } catch (Exception e) {
            log.error("Unexpected error while launching job {}", job.getName(), e);
            return ResponseEntity.internalServerError().body(new JobLaunchResponse(
                    job.getName(),
                    null,
                    effectiveRunId,
                    "FAILED_TO_LAUNCH",
                    "FAILED",
                    e.getMessage()
            ));
        }
    }

    public record JobLaunchResponse(
            String jobName,
            Long jobExecutionId,
            Long runId,
            String status,
            String exitCode,
            String message
    ) {
    }
}
