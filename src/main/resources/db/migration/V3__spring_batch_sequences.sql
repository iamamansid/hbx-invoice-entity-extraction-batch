-- Ensure Spring Batch JDBC metadata sequences exist (PostgreSQL)
-- and are aligned with current table values.

CREATE SEQUENCE IF NOT EXISTS batch_job_instance_seq MAXVALUE 9223372036854775807 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS batch_job_execution_seq MAXVALUE 9223372036854775807 NO CYCLE;
CREATE SEQUENCE IF NOT EXISTS batch_step_execution_seq MAXVALUE 9223372036854775807 NO CYCLE;

DO $$
BEGIN
    IF to_regclass('public.batch_job_instance') IS NOT NULL THEN
        PERFORM setval(
                'batch_job_instance_seq',
                COALESCE((SELECT MAX(job_instance_id) + 1 FROM batch_job_instance), 1),
                false
        );
    END IF;

    IF to_regclass('public.batch_job_execution') IS NOT NULL THEN
        PERFORM setval(
                'batch_job_execution_seq',
                COALESCE((SELECT MAX(job_execution_id) + 1 FROM batch_job_execution), 1),
                false
        );
    END IF;

    IF to_regclass('public.batch_step_execution') IS NOT NULL THEN
        PERFORM setval(
                'batch_step_execution_seq',
                COALESCE((SELECT MAX(step_execution_id) + 1 FROM batch_step_execution), 1),
                false
        );
    END IF;
END
$$;
