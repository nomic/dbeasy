--
-- $1: name
-- $2: spec
  WITH update_
       AS (
            UPDATE simple_store.spec
               SET spec = $2
             WHERE name = $1
         RETURNING simple_store.spec.*
       )
INSERT INTO simple_store.spec(name, spec)
SELECT $1, $2
 WHERE NOT EXISTS ( SELECT 1 FROM update_ );