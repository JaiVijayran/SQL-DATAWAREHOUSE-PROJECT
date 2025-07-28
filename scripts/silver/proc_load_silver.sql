/*
SPECIFICATION:
This stored procedure loads cleaned and transformed data from the bronze schema into the silver schema in the DataWareHouse database. It processes the following tables:
1. silver.crm_cust_info: Loads the most recent customer records, standardizing gender and marital status, and trimming names.
2. silver.crm_prd_info: Drops and recreates the table, loading product data with derived category IDs, standardized product lines, and calculated end dates.
3. silver.crm_sales_details: Drops and recreates the table, loading sales data with validated dates and recalculated sales/price values.
4. silver.erp_cust_AZ12: Loads customer data with cleaned customer IDs, validated birth dates, and standardized gender.
5. silver.erp_LOC_A101: Loads location data with cleaned customer IDs and standardized country names.
6. silver.erp_PX_CAT_G1V2: Loads category data directly without transformations.
- The procedure assumes the DataWareHouse database exists with bronze and silver schemas containing the required tables and columns.
- It uses TRY-CATCH for error handling and SET NOCOUNT ON to suppress row count messages.

WARNING:
- This procedure truncates silver.crm_cust_info, silver.erp_cust_AZ12, silver.erp_LOC_A101, and silver.erp_PX_CAT_G1V2, and drops/recreates silver.crm_prd_info and silver.crm_sales_details, causing data loss in these tables.
- Ensure all bronze tables (crm_cust_info, crm_prd_info, crm_sales_details, erp_cust_AZ12, erp_LOC_A101, erp_PX_CAT_G1V2) exist with valid data and compatible column types.
- Date fields in bronze.crm_sales_details (sls_order_dt, sls_ship_dt, sls_due_dt) must be in YYYYMMDD format as strings or numbers; invalid formats may result in NULL values.
- Execution requires permissions to SELECT on bronze schema, and TRUNCATE, INSERT, DROP, CREATE, and ALTER on silver schema.
- Verify the DataWareHouse database context before execution to avoid schema-related errors.
- The procedure does not return result sets, but errors are captured and raised via TRY-CATCH.
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON; -- Suppress row count messages to avoid unnecessary output

    BEGIN TRY
        -- 1. Load silver.crm_cust_info
        IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
        BEGIN
            -- Truncate the target table to remove existing data
            TRUNCATE TABLE silver.crm_cust_info;

            -- Insert cleaned and transformed customer data
            INSERT INTO silver.crm_cust_info (
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
                TRIM(cst_firstname) AS cst_firstname, -- Remove leading/trailing spaces
                TRIM(cst_lastname) AS cst_lastname, -- Remove leading/trailing spaces
                CASE
                    WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'MALE' -- Standardize gender
                    WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'FEMALE'
                    ELSE 'N/A' -- Handle invalid or missing gender
                END AS cst_gender,
                CASE
                    WHEN cst_marital_status = 'M' THEN 'MARRIED' -- Standardize marital status
                    WHEN cst_marital_status = 'S' THEN 'SINGLE'
                    ELSE 'N/A' -- Handle invalid or missing marital status
                END AS cst_marital_status,
                cst_create_date -- Customer creation date
            FROM (
                -- Subquery to select the most recent record per customer
                SELECT
                    cst_id,
                    cst_key,
                    cst_firstname,
                    cst_lastname,
                    cst_gender,
                    cst_marital_status,
                    cst_create_date,
                    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS last_flag
                FROM bronze.crm_cust_info
                WHERE cst_id IS NOT NULL -- Exclude null customer IDs
            ) T
            WHERE last_flag = 1; -- Keep only the most recent record
        END

        -- 2. Load silver.crm_prd_info
        IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
            DROP TABLE silver.crm_prd_info;

        -- Create the silver.crm_prd_info table
        CREATE TABLE silver.crm_prd_info (
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
        INSERT INTO silver.crm_prd_info (
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
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract and clean category ID
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
            LEAD(prd_start) OVER(PARTITION BY prd_key ORDER BY prd_start) AS prd_end_dt -- Calculate end date
        FROM bronze.crm_prd_info;

        -- 3. Load silver.crm_sales_details
        IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
            DROP TABLE silver.crm_sales_details;

        -- Create the silver.crm_sales_details table
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
                WHEN ISNUMERIC(sls_order_dt) = 1 AND LEN(CAST(sls_order_dt AS VARCHAR)) = 8 
                THEN TRY_CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- Validate and convert date
                ELSE NULL
            END AS sls_order_dt,
            CASE 
                WHEN ISNUMERIC(sls_ship_dt) = 1 AND LEN(CAST(sls_ship_dt AS VARCHAR)) = 8 
                THEN TRY_CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) -- Validate and convert date
                ELSE NULL
            END AS sls_ship_dt,
            CASE 
                WHEN ISNUMERIC(sls_due_dt) = 1 AND LEN(CAST(sls_due_dt AS VARCHAR)) = 8 
                THEN TRY_CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) -- Validate and convert date
                ELSE NULL
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

        -- 4. Load silver.erp_cust_AZ12
        IF OBJECT_ID('silver.erp_cust_AZ12', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE silver.erp_cust_AZ12;

            -- Insert cleaned customer data
            INSERT INTO silver.erp_cust_AZ12 (
                cid,
                bdate,
                gen
            )
            SELECT
                CASE
                    WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix
                    ELSE cid
                END AS cid,
                CASE
                    WHEN TRY_CAST(bdate AS DATE) < '1925-01-01' OR TRY_CAST(bdate AS DATE) > GETDATE() 
                    THEN NULL -- Validate birth date
                    ELSE TRY_CAST(bdate AS DATE)
                END AS bdate,
                CASE 
                    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'FEMALE' -- Standardize gender
                    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'MALE'
                    ELSE 'N/A'
                END AS gen
            FROM bronze.erp_cust_AZ12;
        END

        -- 5. Load silver.erp_LOC_A101
        IF OBJECT_ID('silver.erp_LOC_A101', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE silver.erp_LOC_A101;

            -- Insert cleaned location data
            INSERT INTO silver.erp_LOC_A101 (
                cid,
                country
            )
            SELECT
                REPLACE(cid, '-', '') AS cid, -- Remove hyphens
                CASE
                    WHEN TRIM(country) = 'DE' THEN 'DENMARK' -- Standardize country names
                    WHEN TRIM(country) IN ('US', 'USA') THEN 'UNITED STATES'
                    WHEN TRIM(country) = '' OR country IS NULL THEN 'N/A' -- Handle missing country
                    ELSE TRIM(country)
                END AS country
            FROM bronze.erp_LOC_A101;
        END

        -- 6. Load silver.erp_PX_CAT_G1V2
        IF OBJECT_ID('silver.erp_PX_CAT_G1V2', 'U') IS NOT NULL
        BEGIN
            TRUNCATE TABLE silver.erp_PX_CAT_G1V2;

            -- Insert direct data transfer
            INSERT INTO silver.erp_PX_CAT_G1V2 (
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
        END
    END TRY
    BEGIN CATCH
        -- Capture and raise error details
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH
END;
GO

-- Execute the stored procedure
EXEC silver.load_silver;
