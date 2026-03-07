// src/main/resources/neo4j_constraints.cypher

CREATE CONSTRAINT ON (r:ExtractionRun) ASSERT (r.invoiceNo, r.model, r.runNo) IS NODE KEY;
CREATE CONSTRAINT ON (i:InvoiceNode) ASSERT (i.invoiceNo, i.model) IS NODE KEY;
CREATE CONSTRAINT ON (i:InvoiceNode) ASSERT i.isGroundTruth IS NOT NULL;
