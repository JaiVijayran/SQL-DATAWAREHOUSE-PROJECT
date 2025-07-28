/*
SPECIFICATION:
This script loads cleaned and transformed customer data from the bronze.crm_cust_info table into the silver.crm_cust_info table.
- It selects the most recent record per customer based on cst_create_date.
- It standardizes gender and marital status values and trims whitespace from names.
- The script assumes the DataWareHouse database and bronze schema exist with the required table and columns.

WARNING:
- This script truncates the silver.crm_cust_info table, causing data loss of existing records.
- Ensure the bronze.crm_cust_info table exists and contains valid data.
- Execution requires appropriate permissions (SELECT on bronze, TRUNCATE and INSERT on silver).
- Verify the DataWareHouse database context before running.
*/

-- Set the database context
USE DataWareHouse;

-- Truncate the target table to remove existing data before inserting new records
TRUNCATE TABLE silver.crm_cust_info;

-- Insert cleaned and transformed customer data
INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_gender,
    cst_marital_status,
    cst_create_date
)
SELECT
    cst_id, -- Unique customer identifier
    cst_key, -- Customer key
    TRIM(cst_firstname) AS cst_firstname, -- Remove leading/trailing spaces from first name
    TRIM(cst_lastname) AS cst_lastname, -- Remove leading/trailing spaces from last name
    CASE
        WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'MALE' -- Standardize gender to full word
        WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'FEMALE'
        ELSE 'N/A' -- Handle invalid or missing gender
    END AS cst_gender,
    CASE
        WHEN cst_marital_status = 'M' THEN 'MARRIED' -- Standardize marital status
        WHEN cst_marital_status = 'S' THEN 'SINGLE'
        ELSE 'N/A' -- Handle invalid or missing marital status
    END AS cst_marital_status,
    cst_create_date -- Customer creation date
FROM
(
    -- Subquery to select the most recent record per customer based on cst_create_date
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS last_flag
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL -- Exclude records with null customer IDs
) T
WHERE last_flag = 1; -- Keep only the most recent record for each customer

/*
SPECIFICATION:
This script creates and populates the silver.crm_prd_info table with transformed product data from bronze.crm_prd_info.
- It derives cat_id from prd_key, standardizes prd_line, and calculates prd_end_dt using LEAD.
- The table is dropped and recreated to ensure a clean schema.
- The script assumes the DataWareHouse database and bronze schema exist with the required table and columns.

WARNING:
- This script drops and recreates the silver.crm_prd_info table, causing data loss.
- Ensure the bronze.crm_prd_info table exists with valid prd_key, prd_cost, prd_line, and prd_start columns.
- Execution requires permissions to DROP, CREATE, and INSERT on the silver schema.
- Verify the DataWareHouse database context before running.
*/

-- Set the database context
USE DataWareHouse;

-- Drop the table if it exists to ensure a clean schema
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;

-- Create the silver.crm_prd_info table with defined schema
CREATE TABLE silver.crm_prd_info(
    prd_id INT, -- Product identifier
    cat_id NVARCHAR(50), -- Category identifier derived from prd_key
    prd_key NVARCHAR(50), -- Product key
    prd_nm NVARCHAR(50), -- Product name
    prd_cost INT, -- Product cost
    prd_line NVARCHAR(50), -- Product line description
    prd_start DATE, -- Product start date
    prd_end_dt DATE, -- Product end date
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Data warehouse creation timestamp
);

-- Insert transformed product data
INSERT INTO silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start,
    prd_end_dt
)
SELECT
    prd_id, -- Product identifier
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract and clean category ID from prd_key
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract product key portion
    prd_nm, -- Product name
    ISNULL(prd_cost, 0) AS prd_cost, -- Replace NULL cost with 0
    CASE UPPER(TRIM(prd_line)) -- Standardize product line descriptions
        WHEN 'M' THEN 'MOUNTAIN'
        WHEN 'R' THEN 'ROAD'
        WHEN 'S' THEN 'OTHER SALES'
        WHEN 'T' THEN 'TOURING'
        ELSE 'N/A'
    END AS prd_line,
    CAST(prd_start AS DATE) AS prd_start, -- Ensure date format
    LEAD(prd_start) OVER(PARTITION BY prd_key ORDER BY prd_start) AS prd_end_dt -- Calculate end date using LEAD
FROM bronze.crm_prd_info;

/*
SPECIFICATION:
This script creates and populates the silver.crm_sales_details table with cleaned sales data from bronze.crm_sales_details.
- It validates and converts date fields, recalculates sales and price if invalid, and ensures data consistency.
- The table is dropped and recreated to ensure a clean schema.
- The script assumes the DataWareHouse database and bronze schema exist with the required table and columns.

WARNING:
- This script drops and recreates the silver.crm_sales_details table, causing data loss.
- Ensure the bronze.crm_sales_details table exists with valid data, especially for date fields (YYYYMMDD format).
- Execution requires permissions to DROP, CREATE, and INSERT on the silver schema.
- Verify the DataWareHouse database context before running.
*/

-- Set the database context
USE DataWareHouse;

-- Drop the table if it exists to ensure a clean schema
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;

-- Create the silver.crm_sales_details table with defined schema
CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50), -- Sales order number
    sls_prd_key NVARCHAR(50), -- Product key for sales
    sls_cust_id INT, -- Customer identifier
    sls_order_dt DATE, -- Order date
    sls_ship_dt DATE, -- Ship date
    sls_due_dt DATE, -- Due date
    sls_sales INT, -- Sales amount
    sls_quantity INT, -- Quantity sold
    sls_price INT, -- Price per unit
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Data warehouse creation timestamp
);

-- Insert cleaned and calculated sales data
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num, -- Sales order number
    sls_prd_key, -- Product key
    sls_cust_id, -- Customer identifier
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL -- Validate and convert order date
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE 
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL -- Validate and convert ship date
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE 
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL -- Validate and convert due date
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price) -- Recalculate sales if invalid
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity, -- Quantity sold
    CASE
        WHEN sls_price IS NULL OR sls_price < 0
        THEN sls_sales / NULLIF(sls_quantity, 0) -- Recalculate price if invalid
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;

/*
SPECIFICATION:
This script loads cleaned customer data from bronze.erp_cust_AZ12 into silver.erp_cust_AZ12.
- It removes 'NAS' prefixes from cid, validates birth dates, and standardizes gender values.
- The script assumes the DataWareHouse database and bronze schema exist with the required table and columns.

WARNING:
- This script truncates the silver.erp_cust_AZ12 table, causing data loss of existing records.
- Ensure the bronze.erp_cust_AZ12 table exists with valid cid, bdate, and gen columns.
- Execution requires permissions to TRUNCATE and INSERT on the silver schema and SELECT on the bronze schema.
- The SELECT * statement at the end is for verification and may produce a result set, which could affect automated pipelines.
- Verify the DataWareHouse database context before running.
*/

-- Set the database context
USE DataWareHouse;

-- Truncate the target table to remove existing data
TRUNCATE TABLE silver.erp_cust_AZ12;

-- Insert cleaned customer data
INSERT INTO silver.erp_cust_AZ12(
    cid,
    bdate,
    gen
)
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix from customer ID
        ELSE cid
    END AS cid,
    CASE
        WHEN bdate < '1925-01-01' OR bdate > GETDATE() THEN NULL -- Validate birth date
        ELSE bdate
    END AS bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'FEMALE' -- Standardize gender
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'MALE'
        ELSE 'N/A'
    END AS gen
FROM bronze.erp_cust_AZ12;

-- Select all records for verification purposes
SELECT * FROM silver.erp_cust_AZ12;

/*
SPECIFICATION:
This script loads cleaned location data from bronze.erp_LOC_A101 into silver.erp_LOC_A101.
- It removes hyphens from cid and standardizes country names.
- The script assumes the DataWareHouse database and bronze schema exist with the required table and columns.

WARNING:
- This script truncates the silver.erp_LOC_A101 table, causing data loss of existing records.
- Ensure the bronze.erp_LOC_A101 table exists with valid cid and country columns.
- Execution requires permissions to TRUNCATE and INSERT on the silver schema and SELECT on the bronze schema.
- Verify the DataWareHouse database context before running.
*/

-- Set the database context
USE DataWareHouse;

-- Truncate the target table to remove existing data
TRUNCATE TABLE silver.erp_LOC_A101;

-- Insert cleaned location data
INSERT INTO silver.erp_LOC_A101(
    cid,
    country
)
SELECT
    REPLACE(cid, '-', '') AS cid, -- Remove hyphens from customer ID
    CASE
        WHEN TRIM(country) = 'DE' THEN 'DENMARK' -- Standardize country names
        WHEN TRIM(country) IN ('US', 'USA') THEN 'UNITED STATES'
        WHEN TRIM(country) = '' OR country IS NULL THEN 'N/A' -- Handle missing or empty country
        ELSE TRIM(country)
    END AS country
FROM bronze.erp_LOC_A101;

/*
SPECIFICATION:
This script loads category data from bronze.erp_PX_CAT_G1V2 into silver.erp_PX_CAT_G1V2.
- It performs a direct data transfer without transformations.
- The script assumes the DataWareHouse database and bronze schema exist with the required table and columns.

WARNING:
- This script truncates the silver.erp_PX_CAT_G1V2 table, causing data loss of existing records.
- Ensure the bronze.erp_PX_CAT_G1V2 table exists with valid id, cat, sub_category, and maintenance columns.
- Execution requires permissions to TRUNCATE and INSERT on the silver schema and SELECT on the bronze schema.
- Verify the DataWareHouse database context before running.
*/

-- Set the database context
USE DataWareHouse;

-- Truncate the target table to remove existing data
TRUNCATE TABLE silver.erp_PX_CAT_G1V2;

-- Insert direct data transfer
INSERT INTO silver.erp_PX_CAT_G1V2(
    id,
    cat,
    sub_category,
    maintenance
)
SELECT
    id, -- Category identifier
    cat, -- Category name
    sub_category, -- Sub-category name
    maintenance -- Maintenance flag or value
FROM bronze.erp_PX_CAT_G1V2;
