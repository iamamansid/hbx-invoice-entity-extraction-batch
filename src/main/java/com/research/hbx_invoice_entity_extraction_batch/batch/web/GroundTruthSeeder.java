package com.research.hbx_invoice_entity_extraction_batch.batch.web;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/admin")
@RequiredArgsConstructor
public class GroundTruthSeeder {

    private final Driver neo4jDriver;

    @PostMapping("/seed-ground-truth")
    public ResponseEntity<String> seedGroundTruth() {
        try {
            ClassPathResource resource = new ClassPathResource("ground_truth.csv");
            if (!resource.exists()) {
                return ResponseEntity.badRequest().body("ground_truth.csv not found in classpath.");
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(resource.getInputStream()));
                 Session session = neo4jDriver.session()) {

                // Skip header: invoice_id,date,amount,company
                String line = reader.readLine();

                int count = 0;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    
                    String[] parts = line.split(",", -1);
                    if (parts.length < 4) continue;
                    
                    String invoiceId = parts[0].trim();
                    String date = parts[1].trim();
                    String amount = parts[2].trim();
                    String company = parts[3].trim();

                    try (Transaction tx = session.beginTransaction()) {
                        
                        String invoiceQuery = """
                            MERGE (i:InvoiceNode {invoiceNo: $invoiceNo, model: 'GROUND_TRUTH'})
                            SET i.isGroundTruth = true
                            """;
                        tx.run(invoiceQuery, Map.of("invoiceNo", invoiceId));
                        
                        if (!date.isEmpty()) {
                            String dateQuery = """
                                MATCH (i:InvoiceNode {invoiceNo: $inv, model: 'GROUND_TRUTH'})
                                MERGE (d:DateNode {value: $val, model: 'GROUND_TRUTH'})
                                MERGE (i)-[:HAS_DATE]->(d)
                                """;
                            tx.run(dateQuery, Map.of("inv", invoiceId, "val", date));
                        }
                        
                        if (!amount.isEmpty()) {
                            String amountQuery = """
                                MATCH (i:InvoiceNode {invoiceNo: $inv, model: 'GROUND_TRUTH'})
                                MERGE (a:AmountNode {value: $val, model: 'GROUND_TRUTH'})
                                MERGE (i)-[:HAS_AMOUNT]->(a)
                                """;
                            tx.run(amountQuery, Map.of("inv", invoiceId, "val", amount));
                        }
                        
                        if (!company.isEmpty()) {
                            String companyQuery = """
                                MATCH (i:InvoiceNode {invoiceNo: $inv, model: 'GROUND_TRUTH'})
                                MERGE (c:CompanyNode {name: $val, model: 'GROUND_TRUTH'})
                                MERGE (i)-[:ISSUED_TO]->(c)
                                """;
                            tx.run(companyQuery, Map.of("inv", invoiceId, "val", company));
                        }
                        
                        tx.commit();
                        count++;
                    }
                }
                
                return ResponseEntity.ok("Successfully seeded " + count + " ground truth invoices.");
            }

        } catch (Exception e) {
            log.error("Failed to seed ground truth", e);
            return ResponseEntity.internalServerError().body("Failed to seed: " + e.getMessage());
        }
    }
}
