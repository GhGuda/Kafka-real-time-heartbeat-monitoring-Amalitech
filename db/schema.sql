CREATE TABLE customer_heartbeats (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    heart_rate INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Create indexes to optimize queries on recorded_at and customer_id
CREATE INDEX idx_heartbeats_recorded_at
ON customer_heartbeats (recorded_at);

CREATE INDEX idx_heartbeats_customer_id
ON customer_heartbeats (customer_id);

-- Add a check constraint to ensure heart_rate is within a reasonable range (30-220 bpm)
ALTER TABLE customer_heartbeats
ADD CONSTRAINT chk_heart_rate_range
CHECK (heart_rate BETWEEN 30 AND 220);
