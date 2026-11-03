-- Optional DBA-reviewed index for active paddy-trip discovery.
-- Review table size and existing indexes before applying.
-- Check first: SHOW INDEX FROM shipments;
CREATE INDEX idx_shipments_trip_worker
  ON shipments (status, trip_status, vehicle_id, id);
