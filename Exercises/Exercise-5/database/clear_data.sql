BEGIN;
TRUNCATE TABLE account CASCADE;
TRUNCATE TABLE product CASCADE;
TRUNCATE TABLE "transaction" CASCADE;
COMMIT;