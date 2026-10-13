-- Debezium's signaling table — ad-hoc instructions to the connector.
-- Debezium both reads from it (to find new signals) and writes to it
-- (chunk watermarks during incremental snapshots), so it needs full DML
-- privileges plus coverage in the logical replication publication.

CREATE TABLE public.debezium_signal (
    id   VARCHAR(42)   PRIMARY KEY,
    type VARCHAR(32)   NOT NULL,
    data VARCHAR(2048)
);

ALTER TABLE public.debezium_signal REPLICA IDENTITY FULL;

GRANT SELECT, INSERT, UPDATE, DELETE ON public.debezium_signal TO debezium;

ALTER PUBLICATION dbz_pub ADD TABLE public.debezium_signal;
