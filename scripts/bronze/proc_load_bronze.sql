/*
==============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==============================================================================
Purpose of Script:
  The purpose of this script is to create a stored procedure that first truncates the tables and then
  inserts the data from the both the CRM and ERP source files.  The stored procedure uses 'BULK INSERT'
  to ingest the data.  

Parameters:
  None.
  The stored procedure does not accept any parameters or output any values.

Usage Example:
  EXEC bronze.load_bronze;
==============================================================================
*/
-- Stored procedure to load the bronze tables
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '===========================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===========================================';

		PRINT '-------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: bronze.crm_cust_info';
		-- Truncate the table and then ingest the data from the files into the database
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM '\\depsuse2wf02\Profiles\gbenfer\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		-- First row is the second row
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			-- Locks the table as loading it
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------'

		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM '\\depsuse2wf02\Profiles\gbenfer\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		-- First row is the second row
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			-- Locks the table as loading it
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------'

		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM '\\depsuse2wf02\Profiles\gbenfer\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		-- First row is the second row
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			-- Locks the table as loading it
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------'


		PRINT '-------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM '\\depsuse2wf02\Profiles\gbenfer\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		-- First row is the second row
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			-- Locks the table as loading it
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------'


		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM '\\depsuse2wf02\Profiles\gbenfer\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		-- First row is the second row
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			-- Locks the table as loading it
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------'

		SET @start_time = GETDATE();
		PRINT '>> Trancating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM '\\depsuse2wf02\Profiles\gbenfer\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		-- First row is the second row
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			-- Locks the table as loading it
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------'

		SET @batch_end_time = GETDATE();
		PRINT '============================================'
		PRINT 'Loading Bronze Layer is Completed'
		PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '============================================'
	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '==========================================';
	END CATCH
END
