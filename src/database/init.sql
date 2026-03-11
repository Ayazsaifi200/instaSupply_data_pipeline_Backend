-- Initialize the database schema
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create data_records table to store CSV data
CREATE TABLE IF NOT EXISTS data_records (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    age INTEGER,
    city VARCHAR(255),
    country VARCHAR(255),
    salary DECIMAL(10,2),
    department VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(email) -- Handle re-uploads gracefully by email uniqueness
);

-- Create index on commonly queried fields
CREATE INDEX IF NOT EXISTS idx_data_records_email ON data_records(email);
CREATE INDEX IF NOT EXISTS idx_data_records_created_at ON data_records(created_at);
CREATE INDEX IF NOT EXISTS idx_data_records_department ON data_records(department);

-- Create upload_logs table to track file uploads
CREATE TABLE IF NOT EXISTS upload_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    original_filename VARCHAR(255) NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    records_processed INTEGER NOT NULL DEFAULT 0,
    records_inserted INTEGER NOT NULL DEFAULT 0,
    records_updated INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(50) NOT NULL DEFAULT 'processing',
    error_message TEXT,
    upload_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_time_ms INTEGER
);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for updated_at
CREATE OR REPLACE TRIGGER update_data_records_updated_at 
    BEFORE UPDATE ON data_records 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();