-- Transient provider failures avoid one serving account briefly; unlike quota
-- fences they never make every account unavailable for a new session.
ALTER TABLE account_route_fence ADD COLUMN wall_class TEXT NOT NULL DEFAULT 'quota'
  CHECK (wall_class IN ('quota', 'transient'));
