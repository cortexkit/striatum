-- The account state loader keeps each open admission's sign-in exclusions in
-- memory, so a continuation never reads account_observation. This index lets
-- the loader find the rows it needs without reading every observation: only
-- bound `signed_out` rows enter it, and there are none or a handful, against
-- hundreds of thousands of other observations (454,077 on the live store, all
-- other kinds). Building it reads the table once, during startup migration.
CREATE INDEX account_observation_signed_out
  ON account_observation (principal, admission_id)
  WHERE kind = 'signed_out' AND bound = 1;
