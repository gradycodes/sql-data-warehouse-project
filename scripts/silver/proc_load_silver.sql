/*
===================================================
Silver Layer Stored Procedure (Data Transformation)
===================================================
Purpose of Script:
  The purpose of this script is to take the ingested data from the bronze layer tables, transform
  and clean the data, and then truncate the silver tables and insert the new, clean data.

Parameters:
  None.
  There are no parameters for this stored procedure.
*/


-- Stored procedure truncating then inserting the transformed data from the bronze into the silver tables
-- After transforming a table, always make sure that the DDL is updated from the changes - whether data type or new column
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '====================================='
		PRINT 'Ingesting Data into Silver Tables'
		PRINT '====================================='

		PRINT '-------------------------------------'
		PRINT 'Loading the CRM tables'
		PRINT '-------------------------------------'

		SET @start_time = GETDATE()
		PRINT '>> Truncating Data Into: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info'
		-- Table 'silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
					cst_id,
					cst_key,
					cst_firstname,
					cst_lastname,
					cst_marital_status,
					cst_gndr,
					cst_create_date)

				SELECT
					cst_id,
					cst_key,
					TRIM(cst_firstname) AS cst_firstname,
					TRIM(cst_lastname) AS cst_lastname,
					CASE 
						WHEN TRIM(UPPER(cst_marital_status)) = 'S' THEN 'Single'
						WHEN TRIM(UPPER(cst_marital_status)) = 'M' THEN 'Married'
						ELSE 'n/a'
					END cst_marital_status,
					CASE 
						WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
						WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
						ELSE 'n/a'
					END cst_gndr,
					cst_create_date
				FROM
				(
				SELECT
					*,
					ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
				FROM bronze.crm_cust_info
				)t WHERE flag_last = 1
		SET @end_time = GETDATE()

		PRINT '==================================================='
		PRINT '>> Total Duration for "silver.crm_cust_info": ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '==================================================='


		SET @start_time = GETDATE()
		PRINT '>> Truncating Data Into: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info'
		
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract Category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Extract Product Key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- Take the starting date of the next column for the end date
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE()
		PRINT '==================================================='
		PRINT 'Total Duration for "silver.crm_prd_info" ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '==================================================='


		SET @start_time = GETDATE()
		PRINT '>> Truncating Data Into: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details'
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
			sls_ord_num,
			sls_prd_key, -- Check the integrity of the keys with the other tables to make sure they connect
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END sls_sales,
			sls_quantity,
			CASE
				WHEN sls_price < 0 OR sls_price IS NULL 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE()

		PRINT '==================================================='
		PRINT 'Total Duration for "silver.crm_sales_details ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '==================================================='

		PRINT '*****************************'
		PRINT '-----------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '-----------------------------'
		PRINT '*****************************'

		set @start_time = GETDATE()
		PRINT '>> Truncating Data Into: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END cid,
		CASE
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate, -- Set future birthdates to NULL
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'n/a'
		END AS gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE()

		PRINT '==================================================='
		PRINT 'Total Duration for "silver.erp_cust_az12 ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '==================================================='


		SET @start_time = GETDATE()
		PRINT '>> Truncating Data Into: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT
			REPLACE(cid, '-','') cid,
			CASE	
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE cntry
			END cntry
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE()

		PRINT '==================================================='
		PRINT 'Total Duration for "silver.erp_loc_a101 ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '==================================================='


		SET @start_time = GETDATE()
		PRINT '>> Truncating Data Into: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2
		(id,cat,subcat,maintenance)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE()

		PRINT '==================================================='
		PRINT 'Total Duration for "silver.erp_px_cat_g1v2 ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '==================================================='


		SET @batch_end_time = GETDATE()

		PRINT '=============================='
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds'
		PRINT '=============================='

	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '==========================================';
	END CATCH
END
