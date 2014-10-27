CREATE SCHEMA dbeasy_store;
CREATE TABLE dbeasy_store.spec (
  name text NOT NULL,
  spec json NOT NULL
);

ALTER TABLE ONLY dbeasy_store.spec
  ADD CONSTRAINT "dbeasy_store.spec_pkey" PRIMARY KEY (name);
