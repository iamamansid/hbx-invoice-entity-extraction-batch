-- src/main/resources/db/migration/V2__ground_truth_consensus.sql

CREATE TABLE ground_truth_consensus (
    invoice_id VARCHAR(255) PRIMARY KEY,
    consensus_invoice_no VARCHAR(255),
    consensus_dates TEXT,
    consensus_amounts TEXT,
    consensus_companies TEXT,
    models_agreed INT,
    fields_with_consensus INT,
    seeded_to_neo4j BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT now()
);
