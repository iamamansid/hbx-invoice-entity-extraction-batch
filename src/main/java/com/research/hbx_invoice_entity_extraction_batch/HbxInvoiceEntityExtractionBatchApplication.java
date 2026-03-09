package com.research.hbx_invoice_entity_extraction_batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

@SpringBootApplication
@EnableRetry
public class HbxInvoiceEntityExtractionBatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(HbxInvoiceEntityExtractionBatchApplication.class, args);
	}
}
