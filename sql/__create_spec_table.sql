CREATE SCHEMA simple_store;
CREATE TABLE simple_store.spec (
  name text NOT NULL,
  spec json NOT NULL
);

ALTER TABLE ONLY simple_store.spec
  ADD CONSTRAINT "simple_store.spec_pkey" PRIMARY KEY (name);
