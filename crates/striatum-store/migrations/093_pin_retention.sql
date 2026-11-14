-- Pin retention walks old generations in bounded batches and checks holds by
-- pin, rather than scanning the hold history for every candidate.
CREATE INDEX account_pin_by_created ON account_pin(created_at);
CREATE INDEX account_hold_by_pin ON account_hold(pin_id);
