package com.research.hbx_invoice_entity_extraction_batch.batch.step.groundtruth;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class GroundTruthConsensusStepListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info(
                "Starting step {} for jobExecutionId={} jobName={}",
                stepExecution.getStepName(),
                stepExecution.getJobExecutionId(),
                stepExecution.getJobExecution().getJobInstance().getJobName()
        );
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info(
                "Completed step {} with status={} exitStatus={} readCount={} writeCount={} processSkipCount={} readSkipCount={} writeSkipCount={} filterCount={} commitCount={} rollbackCount={}",
                stepExecution.getStepName(),
                stepExecution.getStatus(),
                stepExecution.getExitStatus().getExitCode(),
                stepExecution.getReadCount(),
                stepExecution.getWriteCount(),
                stepExecution.getProcessSkipCount(),
                stepExecution.getReadSkipCount(),
                stepExecution.getWriteSkipCount(),
                stepExecution.getFilterCount(),
                stepExecution.getCommitCount(),
                stepExecution.getRollbackCount()
        );
        return stepExecution.getExitStatus();
    }
}
