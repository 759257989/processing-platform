-- Stage 1: bootstrap migration. Just enable the pgcrypto extension,
-- which we'll need for gen_random_uuid() in Stage 2's job table.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
