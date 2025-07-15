/*
===============================================================================
 Script Name : bronze_load_procedure.sql
 Purpose     : Defines the stored procedure [bronze].[load_bronze] that
               truncates Bronze‑layer tables and bulk‑loads raw CSV files
               (CRM & ERP sources) into them.  It also logs the duration of
               each load step and captures any run‑time errors.

 ⚠️ WARNING:
 Executing this procedure deletes all existing data in the Bronze tables
 (TRUNCATE) before re‑loading them.  Do **NOT** run in production without
 performing backups or validating the source files and paths.

 Usage:
 EXEC bronze.load_bronze;
===============================================================================
*/

-------------------------------------------------------------------------------
-- Create or replace the load procedure for the Bronze layer
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    ---------------------------------------------------------------------------
    -- Declare timing variables for per‑table loads and the overall batch
    ---------------------------------------------------------------------------
    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;

    ---------------------------------------------------------------------------
    -- Use TRY / CATCH so that any failure is caught and logged
    ---------------------------------------------------------------------------
    BEGIN TRY
        -----------------------------------------------------------------------
        -- Mark the beginning of the batch
        -----------------------------------------------------------------------
        SET @batch_start_time = GETDATE();
        PRINT '===========================================';
        PRINT '         Starting Bronze Layer Load         ';
        PRINT '===========================================';

        /***********************************************************************
         *                         CRM DATA SECTION                           *
         **********************************************************************/

        PRINT '---------------  Loading CRM Tables  ---------------';

        /*----------------------- crm_cust_info ------------------------------*/
        SET @start_time = GETDATE();
        PRINT '>> Truncating  : bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Bulk insert : bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'F:\Omar Folder\Education\Data Analysis\Data with Baraa [Youtube Channel]\SQL Course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'      -- change to actual path
        WITH (
            FIRSTROW = 2,                               -- skip header row
            FIELDTERMINATOR = ',',                      -- CSV delimiter
            TABLOCK                                     -- minimal logging
        );
        SET @end_time = GETDATE();
        PRINT '>> crm_cust_info load duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' s';

        /*----------------------- crm_prd_info -------------------------------*/
        SET @start_time = GETDATE();
        PRINT '>> Truncating  : bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Bulk insert : bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'F:\Omar Folder\Education\Data Analysis\Data with Baraa [Youtube Channel]\SQL Course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> crm_prd_info load duration: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' s';

        /*-------------------- crm_sales_details -----------------------------*/
        SET @start_time = GETDATE();
        PRINT '>> Truncating  : bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Bulk insert : bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'F:\Omar Folder\Education\Data Analysis\Data with Baraa [Youtube Channel]\SQL Course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> crm_sales_details load duration: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' s';

        /***********************************************************************
         *                         ERP DATA SECTION                           *
         **********************************************************************/

        PRINT '---------------  Loading ERP Tables  ---------------';

        /*---------------------- erp_cust_az12 --------------------------------*/
        SET @start_time = GETDATE();
        PRINT '>> Truncating  : bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Bulk insert : bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'F:\Omar Folder\Education\Data Analysis\Data with Baraa [Youtube Channel]\SQL Course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> erp_cust_az12 load duration: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' s';

        /*---------------------- erp_loc_a101 ---------------------------------*/
        SET @start_time = GETDATE();
        PRINT '>> Truncating  : bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Bulk insert : bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'F:\Omar Folder\Education\Data Analysis\Data with Baraa [Youtube Channel]\SQL Course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> erp_loc_a101 load duration: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' s';

        /*-------------------- erp_px_cat_g1v2 --------------------------------*/
        SET @start_time = GETDATE();
        PRINT '>> Truncating  : bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Bulk insert : bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'F:\Omar Folder\Education\Data Analysis\Data with Baraa [Youtube Channel]\SQL Course\sql-ultimate-course\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> erp_px_cat_g1v2 load duration: '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' s';

        -----------------------------------------------------------------------
        -- Print overall batch duration
        -----------------------------------------------------------------------
        SET @batch_end_time = GETDATE();
        PRINT '===========================================';
        PRINT ' Bronze Layer Load Completed Successfully ';
        PRINT '   Total Batch Duration: ' 
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) 
                AS NVARCHAR) + ' s';
        PRINT '===========================================';

    END TRY
    BEGIN CATCH
        -----------------------------------------------------------------------
        -- Log error details if anything goes wrong
        -----------------------------------------------------------------------
        PRINT '===========================================';
        PRINT '   ERROR during Bronze Layer Load';
        PRINT '   MESSAGE : ' + ERROR_MESSAGE();
        PRINT '   NUMBER  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '   STATE   : ' + CAST(ERROR_STATE()  AS NVARCHAR);
        PRINT '===========================================';
        -- Optional: RETHROW error to calling session
        -- THROW;
    END CATCH
END;
GO
