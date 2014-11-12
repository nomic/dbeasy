--
-- $1: schema
-- $2: table
--

SELECT *
  FROM information_schema.tables
 WHERE table_schema = $1
   AND table_name = $2;