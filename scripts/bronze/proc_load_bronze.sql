/*
Specification and Purpose:
This SQL stored procedure, bronze.load_bronze, is designed to populate tables in the bronze schema of a data warehouse by loading data from CSV files sourced from CRM and ERP systems. 
It truncates existing data in the target tables and uses the BULK INSERT operation to load customer, product, sales, and ERP-related data (customer demographics, location, and product catalog) into their respective tables: bronze.crm_cust_info, bronze.crm_prd_info, bronze.crm_sales_details, bronze.erp_cust_AZ12, bronze.erp_LOC_A101, and bronze.erp_PX_CAT_G1V2. 
The procedure includes print statements to log the progress of loading CRM and ERP tables.

Warning:
- This procedure truncates all specified tables before loading new data, which will result in the loss of existing data in these tables. Ensure that critical data is backed up before execution.
- The file paths specified in the BULK INSERT statements are hardcoded to a specific local directory ('C:\Users\JAI VIJAYRAN\...'). Verify that these files exist and are accessible on the server where the SQL Server instance is running, and update the paths if necessary.
- Ensure that the SQL Server service account has read permissions for the specified file paths.
- The CSV files must match the schema of the target tables, including column count, data types, and field lengths, to avoid errors during the BULK INSERT operation.
- The bronze schema must exist in the database, and the tables must be created prior to running this procedure (e.g., using a table creation script).

How to Execute:
1. Ensure the bronze schema and all target tables (bronze.crm_cust_info, bronze.crm_prd_info, bronze.crm_sales_details, bronze.erp_cust_AZ12, bronze.erp_LOC_A101, bronze.erp_PX_CAT_G1V2) exist in the database.
2. Verify that the CSV files are located at the specified paths and are accessible by the SQL Server service account.
3. Open SQL Server Management Studio (SSMS) or another SQL client connected to the target database.
4. Execute the stored procedure by running the following command:
   **EXEC bronze.load_bronze;**
5. Monitor the output in the Messages tab of SSMS to confirm successful loading of CRM and ERP tables.

*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    --TO TRUNCATE AND LOAD DATA INTO TABLE bronze.crm_cust_info .
    PRINT'======================================================';
    PRINT'LOADING THE BRONZE LAYER';
    PRINT'======================================================';
    PRINT'------------------------------------------------------';
    PRINT'LOADING CRM TABLES';
    PRINT'------------------------------------------------------';
    TRUNCATE TABLE bronze.crm_cust_info;

    BULK INSERT bronze.crm_cust_info
    from 'C:\Users\JAI VIJAYRAN\OneDrive\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH 
    (
           FIRSTROW=2,
           FIELDTERMINATOR=',',
           TABLOCK
    );

    --TO TRUNCATE AND LOAD DATA INTO TABLE bronze.crm_prd_info .

    TRUNCATE TABLE bronze.crm_prd_info;

    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\JAI VIJAYRAN\OneDrive\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH
    (
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );

    --TO TRUNCATE AND LOAD DATA INTO TABLE bronze.crm_sales_details.

    TRUNCATE TABLE bronze.crm_sales_details;

    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\JAI VIJAYRAN\OneDrive\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH
    (
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    )

    PRINT 'LOADING THE ERP TABLES';
     PRINT'------------------------------------------------------';

    --TO TRUNCATE AND LOAD DATA INTO TABLE bronze.erp_cust_AZ12.

    TRUNCATE TABLE bronze.erp_cust_AZ12;

    BULK INSERT bronze.erp_cust_AZ12
    FROM 'C:\Users\JAI VIJAYRAN\OneDrive\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH
    (
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    )

    --TO TRUNCATE AND LOAD DATA INTO TABLE bronze.erp_LOC_A101.

    TRUNCATE TABLE bronze.erp_LOC_A101;

    BULK INSERT bronze.erp_LOC_A101
    FROM 'C:\Users\JAI VIJAYRAN\OneDrive\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH
    (
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    )

    --TO TRUNCATE AND LOAD DATA INTO TABLE bronze.erp_PX_CAT_G1V2.

    TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

    BULK INSERT bronze.erp_PX_CAT_G1V2
    FROM 'C:\Users\JAI VIJAYRAN\OneDrive\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH
    (
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    )

END 
