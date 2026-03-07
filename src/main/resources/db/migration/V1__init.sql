-- src/main/resources/db/migration/V1__init.sql

CREATE TABLE batch_run (
    run_id UUID PRIMARY KEY,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    total_invoices INT,
    status VARCHAR(50)
);

CREATE TABLE invoice (
    invoice_id VARCHAR(255) PRIMARY KEY,
    gcs_path VARCHAR(500),
    raw_text TEXT,
    normalized_text TEXT,
    ocr_status VARCHAR(50),
    extraction_status VARCHAR(50),
    evaluation_status VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE extraction_run (
    invoice_id VARCHAR(255),
    model_name VARCHAR(255),
    run_number INT,
    extracted_json JSONB,
    latency_ms INT,
    token_count INT,
    extraction_status VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP,
    PRIMARY KEY (invoice_id, model_name, run_number)
);

CREATE TABLE evaluation_result (
    invoice_id VARCHAR(255),
    model_name VARCHAR(255),
    run_number INT,
    date_accuracy DOUBLE PRECISION,
    date_f1 DOUBLE PRECISION,
    amount_accuracy DOUBLE PRECISION,
    amount_f1 DOUBLE PRECISION,
    company_accuracy DOUBLE PRECISION,
    company_f1 DOUBLE PRECISION,
    overall_entity_accuracy DOUBLE PRECISION,
    relation_completeness DOUBLE PRECISION,
    query_answerability DOUBLE PRECISION,
    graph_idempotent BOOLEAN,
    consistency_score DOUBLE PRECISION,
    created_at TIMESTAMP,
    PRIMARY KEY (invoice_id, model_name, run_number)
);
