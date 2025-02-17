/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
EXECUTE bronze.load_bronze
CREATE OR ALTER PROCEDURE bronze.load_bronze as 
BEGIN
	DECLARE @start_time DATETIME , @end_date DATETIME;
	DECLARE @start_bronze_time datetime, @end_bronze_time datetime;

	
	BEGIN TRY 
	SET @start_bronze_time=GETDATE();
		PRINT'==============================================';
		PRINT'LAODING BRONZE TABLES ';
		PRINT'==============================================';

		PRINT'==============================================';
		PRINT'-----------CRM TABLES-------------------------';
		PRINT'==============================================';
		PRINT'>>>>Table Cust_info';

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info
		Bulk INSERT bronze.crm_cust_info
		FROM 'C:\Users\Hp\Desktop\Formations\SQL-DataWerhouseProject\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_date=GETDATE();
		PRINT'loading duration: '+CAST(DATEDIFF(second,@start_time,@end_date) AS NVARCHAR)+'secands'
		PRINT'-------------------------------------------------';
		SELECT count(*) FROM bronze.crm_cust_info

		SET @start_time=GETDATE();
		PRINT'>>>>Table prd_info';
		TRUNCATE TABLE bronze.crm_prd_info
		Bulk INSERT bronze.crm_prd_info
		FROM 'C:\Users\Hp\Desktop\Formations\SQL-DataWerhouseProject\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_date=GETDATE();
		PRINT'loading duration: '+CAST(DATEDIFF(second,@start_time,@end_date) AS NVARCHAR)+'secands'
		PRINT'-------------------------------------------------';
		SELECT count(*) FROM bronze.crm_prd_info

		PRINT'>>>>Table sales_details';
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details
		Bulk INSERT bronze.crm_sales_details
		FROM 'C:\Users\Hp\Desktop\Formations\SQL-DataWerhouseProject\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_date=GETDATE();
		PRINT'loading duration: '+CAST(DATEDIFF(second,@start_time,@end_date) AS NVARCHAR)+'secands'
		PRINT'-------------------------------------------------';

		SELECT COUNT(*) FROM bronze.crm_sales_details

		---ERP TABLES
		PRINT'==============================================';
		PRINT'-----------ERP TABLES-------------------------';
		PRINT'==============================================';
	
		PRINT'>>>>Table cust_az12';
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12
		Bulk INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Hp\Desktop\Formations\SQL-DataWerhouseProject\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_date=GETDATE();
		PRINT'loading duration: '+CAST(DATEDIFF(second,@start_time,@end_date) AS NVARCHAR)+'secands'
		PRINT'-------------------------------------------------';

		SELECT count(*) FROM bronze.erp_cust_az12

		PRINT'>>>>Table loc_a101';
		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101
		Bulk INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Hp\Desktop\Formations\SQL-DataWerhouseProject\LOC_A101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_date=GETDATE();
		PRINT'loading duration: '+CAST(DATEDIFF(second,@start_time,@end_date) AS NVARCHAR)+'secands'
		PRINT'-------------------------------------------------';

		SELECT count(*) FROM bronze.erp_loc_a101

		PRINT'>>>>Table px_cat_g1v2';

		SET @start_time=GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		Bulk INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Hp\Desktop\Formations\SQL-DataWerhouseProject\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_date=GETDATE();
		PRINT'loading duration: '+CAST(DATEDIFF(second,@start_time,@end_date) AS NVARCHAR)+'secands'
		PRINT'-------------------------------------------------';

		SELECT count(*) FROM bronze.erp_px_cat_g1v2

		SET @end_bronze_time=GETDATE()
		PRINT'Completion time :'+CAST(DATEDIFF(second,@start_bronze_time,@end_bronze_time) as nvarchar)+' seconds';

	END TRY 
	BEGIN CATCH 
	PRINT'==========================================';
	PRINT'ERROR OCURED DURING BRONZE LAYER ';
	PRINT'ERROR message'+ERROR_MESSAGE();
	PRINT'ERROR message'+ cast(ERROR_NUMBER() as  nvarchar);
	END CATCH

END
