// src/main/java/com/research/hbx_invoice_entity_extraction_batch/batch/config/GroundTruthBatchConfig.java
package com.research.hbx_invoice_entity_extraction_batch.batch.config;

import com.research.hbx_invoice_entity_extraction_batch.batch.model.jpa.GroundTruthConsensus;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.groundtruth.GroundTruthConsensusItemReader;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.groundtruth.GroundTruthConsensusProcessor;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.groundtruth.GroundTruthConsensusSkipListener;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.groundtruth.GroundTruthConsensusStepListener;
import com.research.hbx_invoice_entity_extraction_batch.batch.step.groundtruth.GroundTruthConsensusWriter;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
public class GroundTruthBatchConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final GroundTruthConsensusProcessor groundTruthConsensusProcessor;
    private final GroundTruthConsensusWriter groundTruthConsensusWriter;
    private final GroundTruthConsensusStepListener groundTruthConsensusStepListener;
    private final GroundTruthConsensusSkipListener groundTruthConsensusSkipListener;

    @Bean
    public GroundTruthConsensusItemReader groundTruthConsensusItemReader(DataSource dataSource) {
        return new GroundTruthConsensusItemReader(dataSource);
    }

    @Bean
    public Step groundTruthConsensusStep(GroundTruthConsensusItemReader groundTruthConsensusItemReader) {
        return new StepBuilder("groundTruthConsensusStep", jobRepository)
                .<String, GroundTruthConsensus>chunk(10, transactionManager)
                .reader(groundTruthConsensusItemReader)
                .processor(groundTruthConsensusProcessor)
                .writer(groundTruthConsensusWriter)
                .listener(groundTruthConsensusStepListener)
                .listener(groundTruthConsensusSkipListener)
                .faultTolerant()
                .skip(Exception.class)
                .skipLimit(50)
                .noSkip(DataAccessException.class)
                .build();
    }

    @Bean
    public Job groundTruthConsensusJob(Step groundTruthConsensusStep) {
        return new JobBuilder("groundTruthConsensusJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(groundTruthConsensusStep)
                .build();
    }
}
