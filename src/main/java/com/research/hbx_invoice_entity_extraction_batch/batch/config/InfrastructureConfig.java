// c:\Users\amans\OneDrive\Documents\Repos\hbx-invoice-entity-extraction-batch\src\main\java\com\research\hbx_invoice_entity_extraction_batch\batch\config\InfrastructureConfig.java
package com.research.hbx_invoice_entity_extraction_batch.batch.config;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.TimeUnit;

@Configuration
public class InfrastructureConfig {

    @Bean
    public RestTemplate restTemplate() {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(30_000);
        requestFactory.setReadTimeout(120_000);
        return new RestTemplate(requestFactory);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean(destroyMethod = "close")
    public Driver neo4jDriver(
            @Value("${spring.neo4j.uri}") String uri,
            @Value("${spring.neo4j.authentication.username}") String username,
            @Value("${spring.neo4j.authentication.password}") String password,
            @Value("${batch.neo4j.driver.max-connection-pool-size:150}") int maxConnectionPoolSize,
            @Value("${batch.neo4j.driver.connection-acquisition-timeout-ms:120000}") long connectionAcquisitionTimeoutMs,
            @Value("${batch.neo4j.driver.connection-timeout-ms:30000}") long connectionTimeoutMs,
            @Value("${batch.neo4j.driver.max-connection-lifetime-ms:1800000}") long maxConnectionLifetimeMs,
            @Value("${batch.neo4j.driver.max-transaction-retry-time-ms:30000}") long maxTransactionRetryTimeMs,
            @Value("${batch.neo4j.driver.enable-leaked-session-logging:true}") boolean enableLeakedSessionLogging) {

        Config.ConfigBuilder configBuilder = Config.builder()
                .withMaxConnectionPoolSize(Math.max(1, maxConnectionPoolSize))
                .withConnectionAcquisitionTimeout(Math.max(1_000, connectionAcquisitionTimeoutMs), TimeUnit.MILLISECONDS)
                .withConnectionTimeout(Math.max(1_000, connectionTimeoutMs), TimeUnit.MILLISECONDS)
                .withMaxConnectionLifetime(Math.max(60_000, maxConnectionLifetimeMs), TimeUnit.MILLISECONDS)
                .withMaxTransactionRetryTime(Math.max(1_000, maxTransactionRetryTimeMs), TimeUnit.MILLISECONDS);

        if (enableLeakedSessionLogging) {
            configBuilder.withLeakedSessionsLogging();
        }

        return GraphDatabase.driver(uri, AuthTokens.basic(username, password), configBuilder.build());
    }
}
