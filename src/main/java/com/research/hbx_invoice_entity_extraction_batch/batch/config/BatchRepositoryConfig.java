package com.research.hbx_invoice_entity_extraction_batch.batch.config;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableBatchProcessing
@EnableJdbcJobRepository(
        dataSourceRef = "dataSource",
        transactionManagerRef = "transactionManager"
)
public class BatchRepositoryConfig extends DefaultBatchConfiguration {

    @Value("${batch.launcher.core-pool-size:1}")
    private int launcherCorePoolSize;

    @Value("${batch.launcher.max-pool-size:2}")
    private int launcherMaxPoolSize;

    @Value("${batch.launcher.queue-capacity:20}")
    private int launcherQueueCapacity;

    @Override
    protected TaskExecutor getTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setThreadNamePrefix("batch-job-");
        executor.setCorePoolSize(Math.max(1, launcherCorePoolSize));
        executor.setMaxPoolSize(Math.max(1, launcherMaxPoolSize));
        executor.setQueueCapacity(Math.max(1, launcherQueueCapacity));
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }
}
