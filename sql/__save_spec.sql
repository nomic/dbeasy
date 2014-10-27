--
-- $1: name
-- $2: spec
  WITH update_
       AS (
            UPDATE dbeasy_store.spec
               SET spec = $2
             WHERE name = $1
         RETURNING dbeasy_store.spec.*
       )
INSERT INTO dbeasy_store.spec(name, spec)
SELECT $1, $2
 WHERE NOT EXISTS ( SELECT 1 FROM update_ );