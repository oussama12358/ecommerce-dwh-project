-- Create database if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ecommerce_dwh') THEN
        CREATE DATABASE ecommerce_dwh;
    END IF;
END $$;

\c ecommerce_dwh;

-- Begin transaction
BEGIN;

-- Drop tables if they exist (in reverse order of dependencies)
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_region CASCADE;

-- Create Dimension Tables
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_product_id UNIQUE (product_id)
);

CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    day_of_week VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_full_date UNIQUE (full_date)
);

CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    segment VARCHAR(50),
    country VARCHAR(100),
    city VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_customer_id UNIQUE (customer_id)
);

CREATE TABLE dim_region (
    region_key SERIAL PRIMARY KEY,
    region_name VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    continent VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_region_name UNIQUE (region_name)
);

-- Create Fact Table
CREATE TABLE fact_sales (
    sale_key SERIAL PRIMARY KEY,
    product_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    region_key INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    discount DECIMAL(4,2) NOT NULL DEFAULT 0.00 CHECK (discount BETWEEN 0 AND 1),
    total_amount DECIMAL(12,2) NOT NULL CHECK (total_amount >= 0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_product FOREIGN KEY (product_key) REFERENCES dim_product (product_key),
    CONSTRAINT fk_date FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_customer FOREIGN KEY (customer_key) REFERENCES dim_customer (customer_key),
    CONSTRAINT fk_region FOREIGN KEY (region_key) REFERENCES dim_region (region_key)
);

-- Create Indexes for Performance
CREATE INDEX idx_fact_product ON fact_sales(product_key);
CREATE INDEX idx_fact_date ON fact_sales(date_key);
CREATE INDEX idx_fact_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_region ON fact_sales(region_key);
CREATE INDEX idx_product_category ON dim_product(category);
CREATE INDEX idx_customer_segment ON dim_customer(segment);
CREATE INDEX idx_region_country ON dim_region(country);

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers to update timestamps
CREATE TRIGGER update_product_modtime
BEFORE UPDATE ON dim_product
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_customer_modtime
BEFORE UPDATE ON dim_customer
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_region_modtime
BEFORE UPDATE ON dim_region
FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- Commit the transaction
COMMIT;

-- Add comments for documentation
COMMENT ON TABLE dim_product IS 'Product dimension table containing product information';
COMMENT ON TABLE dim_date IS 'Date dimension table for time-based analysis';
COMMENT ON TABLE dim_customer IS 'Customer dimension table with customer details';
COMMENT ON TABLE dim_region IS 'Geographical region dimension';
COMMENT ON TABLE fact_sales IS 'Fact table containing sales transactions';

-- Create a view for common sales analysis
CREATE OR REPLACE VIEW vw_sales_summary AS
SELECT 
    d.year,
    d.month,
    r.region_name,
    p.category,
    c.segment,
    COUNT(*) AS total_orders,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.total_amount) / NULLIF(COUNT(DISTINCT f.customer_key), 0) AS avg_revenue_per_customer
FROM 
    fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    JOIN dim_region r ON f.region_key = r.region_key
    JOIN dim_product p ON f.product_key = p.product_key
    JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY 
    d.year, d.month, r.region_name, p.category, c.segment;

-- Create a materialized view for frequently accessed reports
CREATE MATERIALIZED VIEW mv_monthly_sales AS
SELECT 
    d.year,
    d.month,
    SUM(f.total_amount) AS monthly_revenue,
    COUNT(DISTINCT f.customer_key) AS unique_customers,
    COUNT(*) AS total_orders
FROM 
    fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
GROUP BY 
    d.year, d.month
ORDER BY 
    d.year, d.month;

-- Create index on materialized view
CREATE UNIQUE INDEX idx_mv_monthly_sales ON mv_monthly_sales (year, month);

-- Create a function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_materialized_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_sales;
END;
$$ LANGUAGE plpgsql;

-- Grant necessary permissions (adjust as needed)
GRANT USAGE ON SCHEMA public TO ecommerce_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ecommerce_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO ecommerce_user;