--
-- $1: schema
-- $2: table
--
SELECT *
  FROM information_schema.columns
 WHERE table_schema = $1
   AND table_name = $2
 ORDER BY ordinal_position ASC;