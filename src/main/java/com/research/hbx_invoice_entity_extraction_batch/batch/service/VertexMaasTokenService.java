package com.research.hbx_invoice_entity_extraction_batch.batch.service;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Slf4j
@Service
public class VertexMaasTokenService {

    private static final List<String> CLOUD_PLATFORM_SCOPE =
            List.of("https://www.googleapis.com/auth/cloud-platform");

    private final GoogleCredentials credentials;
    private final String fallbackAccessToken;

    public VertexMaasTokenService(
            @Value("${gcp.credentials-path:}") String credentialsPath,
            @Value("${vertex.maas.access-token:}") String fallbackAccessToken,
            ResourceLoader resourceLoader) {
        this.fallbackAccessToken = fallbackAccessToken != null ? fallbackAccessToken.trim() : "";
        this.credentials = initializeCredentials(credentialsPath, resourceLoader);
    }

    public synchronized String getFreshAccessToken() {
        if (credentials != null) {
            try {
                AccessToken accessToken = credentials.refreshAccessToken();
                if (accessToken != null
                        && accessToken.getTokenValue() != null
                        && !accessToken.getTokenValue().isBlank()) {
                    return accessToken.getTokenValue();
                }
            } catch (IOException e) {
                log.warn("Failed to refresh Vertex MAAS access token from Google credentials: {}", e.getMessage());
            }
        }

        if (!fallbackAccessToken.isBlank()) {
            return fallbackAccessToken;
        }

        throw new IllegalStateException(
                "Unable to obtain Vertex MAAS access token. Configure ADC, gcp.credentials-path, " +
                        "or vertex.maas.access-token."
        );
    }

    private GoogleCredentials initializeCredentials(String credentialsPath, ResourceLoader resourceLoader) {
        try {
            if (credentialsPath != null && !credentialsPath.isBlank()) {
                Resource resource = resourceLoader.getResource(credentialsPath);
                if (resource.exists()) {
                    try (InputStream inputStream = resource.getInputStream()) {
                        return GoogleCredentials.fromStream(inputStream).createScoped(CLOUD_PLATFORM_SCOPE);
                    }
                }
            }

            return GoogleCredentials.getApplicationDefault().createScoped(CLOUD_PLATFORM_SCOPE);
        } catch (IOException e) {
            log.warn("Google credentials could not be initialized for Vertex MAAS token refresh: {}", e.getMessage());
            return null;
        }
    }
}
