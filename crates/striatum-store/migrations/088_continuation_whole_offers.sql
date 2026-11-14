-- A continuation row is an offer only when it has all three of its decision,
-- reserved admission and deadline; the routing writer always writes the three
-- together or none of them. A row with only some of them is not an offer to
-- any reader: clear the partial columns and keep the row's replay body, so it
-- reads as the hold or refusal record it effectively is. The in-memory account
-- state refuses to load such a row if one ever appears again.
UPDATE account_continue
   SET decision_id = NULL, reservation_id = NULL, valid_until_ms = NULL
 WHERE NOT ((decision_id IS NULL AND reservation_id IS NULL AND valid_until_ms IS NULL)
         OR (decision_id IS NOT NULL AND reservation_id IS NOT NULL
             AND valid_until_ms IS NOT NULL));
