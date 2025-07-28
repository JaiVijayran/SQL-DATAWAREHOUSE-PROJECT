/*
SPECIFICATION:
This script contains a series of SQL queries to validate and clean data in the bronze schema tables (crm_cust_info, crm_prd_info, crm_sales_details, erp_cust_AZ12, erp_LOC_A101, erp_PX_CAT_G1V2) within the DataWareHouse database. The queries check for:
1. Duplicates and NULLs in primary keys (e.g., cst_id, prd_id, sls_ord_num).
2. Unwanted spaces in string fields (e.g., cst_firstname, prd_nm, cat).
3. Data standardization and consistency (e.g., gender, marital status, country, product line).
4. Invalid dates (e.g., bdate, sls_order_dt, prd_end_dt < prd_start).
5. Data consistency between related fields (e.g., sls_sales = sls_quantity * sls_price).
6. Data quality and relationships between tables (e.g., joining erp_cust_AZ12 with crm_cust_info).
- The script assumes the DataWareHouse database exists with bronze and silver schemas containing the required tables and columns.
- Most queries expect no results for data quality issues (e.g., no duplicates, no unwanted spaces), indicating clean data if empty.

WARNING:
- These queries are diagnostic and do not modify data, but they may return large result sets if data quality issues are significant, potentially impacting performance.
- Ensure the bronze and silver schemas exist with the specified tables and columns.
- Execution requires SELECT permissions on the bronze and silver schemas.
- Verify the DataWareHouse database context before running to avoid schema-related errors.
- Some queries (e.g., SELECT * or joins) may return sensitive data; ensure compliance with data privacy policies.
- Invalid data formats (e.g., non-YYYYMMDD strings in sls_order_dt) may cause errors or unexpected NULLs in date validations.
*/

-- Set the database context
USE DataWareHouse;

-- CHECKING FOR DUPLICATES IN cst_id
-- Expectation: No results if cst_id is unique and non-NULL
SELECT
    cst_id,
    COUNT(*) AS totalcst_id
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Inspect specific customer record for duplicate analysis
-- Example: Checking details for cst_id = 29466
SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466;

-- Identify non-latest records for duplicate cst_id
-- Expectation: Returns older records to be excluded in silver layer
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS last_flag
    FROM bronze.crm_cust_info
) T
WHERE last_flag != 1;

-- CHECK FOR UNWANTED SPACES IN STRING FIELDS
-- Expectation: No results if fields are properly trimmed
SELECT
    cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT
    cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT
    cst_marital_status
FROM bronze.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status);

SELECT
    cst_gender
FROM bronze.crm_cust_info
WHERE cst_gender != TRIM(cst_gender);

-- CHECK DATA STANDARDIZATION AND CONSISTENCY
-- List distinct gender values to verify standardization
-- Expectation: Only expected values (e.g., 'M', 'F', or variants)
SELECT DISTINCT
    cst_gender
FROM bronze.crm_cust_info;

-- List distinct marital status values to verify standardization
-- Expectation: Only expected values (e.g., 'M', 'S', or variants)
SELECT DISTINCT
    cst_marital_status
FROM bronze.crm_cust_info;

-- Inspect all product data for initial review
-- Used for manual validation of prd_id, prd_key, prd_nm, etc.
SELECT *
FROM bronze.crm_prd_info;

-- CHECK FOR NULLS AND DUPLICATES IN PRIMARY KEY (prd_id)
-- Expectation: No results if prd_id is unique and non-NULL
SELECT
    prd_id,
    COUNT(*) AS total_prd_id
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- CHECK FOR UNWANTED SPACES IN prd_nm
-- Expectation: No results if prd_nm is properly trimmed
SELECT *
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- CHECK FOR NULLS AND NEGATIVE VALUES IN prd_cost
-- Expectation: No results if prd_cost is non-NULL and non-negative
SELECT
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL;

SELECT
    prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0;

-- CHECK PRODUCT LINE STANDARDIZATION
-- List distinct prd_line values to verify consistency
-- Expectation: Only expected values (e.g., 'M', 'R', 'S', 'T')
SELECT DISTINCT
    prd_line
FROM bronze.crm_prd_info;

-- CHECK FOR INVALID DATE ORDERS
-- Expectation: No results if prd_end_dt >= prd_start
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start;

-- Validate end date calculation using LEAD for specific prd_key values
-- Example: Checking prd_key 'AC-HE-HL-U509-R' and 'AC-HE-HL-U509'
SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start,
    prd_end_dt,
    LEAD(prd_start) OVER(PARTITION BY prd_key ORDER BY prd_start) AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

-- CHECK FOR UNWANTED SPACES IN sls_ord_num
-- Expectation: No results if sls_ord_num is properly trimmed
SELECT *
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- CHECK FOR INVALID DATES IN SALES DATA
-- Expectation: Returns records with invalid sls_order_dt (non-YYYYMMDD or zero)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt = 0 OR LEN(sls_order_dt) != 8;

-- Inspect sls_order_dt, converting zeros to NULL
SELECT
    NULLIF(sls_order_dt, 0) AS sls_date
FROM bronze.crm_sales_details;

-- Check for invalid sls_ship_dt
-- Expectation: Returns records with invalid sls_ship_dt (non-YYYYMMDD or zero)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8;

-- Check for invalid sls_due_dt
-- Expectation: Returns records with invalid sls_due_dt (non-YYYYMMDD or zero)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_due_dt = 0 OR LEN(sls_due_dt) != 8;

-- Check for logical date inconsistencies
-- Expectation: No results if order date is before ship or due date
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- CHECK DATA CONSISTENCY BETWEEN SALES, QUANTITY, AND PRICE
-- Expectation: Identify records where sls_sales != sls_quantity * |sls_price|
SELECT
    sls_quantity,
    sls_sales,
    sls_price
FROM bronzeავ

System: bronze.crm_sales_details
WHERE ABS(sls_sales) != ABS(sls_quantity * sls_price);

-- Check for inconsistencies where sls_sales != sls_price (ignoring quantity)
-- Expectation: Identify records with mismatched sales and price values
SELECT
    sls_quantity,
    sls_sales,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_price;

-- CHECK QUANTITY VALUES FOR CONSISTENCY
-- List distinct sls_quantity values to verify range and validity
SELECT DISTINCT
    sls_quantity
FROM bronze.crm_sales_details;

-- VALIDATE CUSTOMER DATA CONSISTENCY BETWEEN TABLES
-- Join erp_cust_AZ12 with crm_cust_info to check key alignment
-- Expectation: Verify cid matches cst_key after cleaning
SELECT *
FROM (
    SELECT
        bdate,
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END AS cid,
        gen
    FROM bronze.erp_cust_AZ12
) T
INNER JOIN silver.crm_cust_info
    ON T.cid = silver.crm_cust_info.cst_key;

-- Inspect all customer data in silver layer for reference
SELECT *
FROM silver.crm_cust_info;

-- CHECK FOR INVALID BIRTH DATES
-- Expectation: Returns records with bdate before 1925 or after current date
SELECT *
FROM bronze.erp_cust_AZ12
WHERE bdate < '1925-01-01' OR bdate > GETDATE();

-- CHECK GENDER STANDARDIZATION IN erp_cust_AZ12
-- Expectation: Only expected values (e.g., 'F', 'M', 'FEMALE', 'MALE')
SELECT DISTINCT
    gen
FROM bronze.erp_cust_AZ12;

-- CHECK CUSTOMER ID CONSISTENCY IN erp_LOC_A101
-- Inspect cid and cleaned version to identify discrepancies
SELECT
    cid,
    REPLACE(cid, '-', '') AS cid_cleaned,
    country
FROM bronze.erp_LOC_A101;

-- CHECK CONSISTENCY OF COUNTRY VALUES
-- Expectation: List distinct country values to verify standardization
SELECT DISTINCT
    country
FROM bronze.erp_LOC_A101;

-- CHECK FOR UNWANTED SPACES IN erp_PX_CAT_G1V2
-- Expectation: No results if cat, sub_category, and maintenance are properly trimmed
SELECT *
FROM bronze.erp_PX_CAT_G1V2
WHERE cat != TRIM(cat) OR sub_category != TRIM(sub_category) OR maintenance != TRIM(maintenance);
