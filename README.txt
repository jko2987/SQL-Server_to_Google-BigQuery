*************************************************************************************************************************
HOW TO INSTALL:

	1 - In the directory \***relative_path***\SQL_to_BQ\dist\ 
	2 - Run the sql_to_bq.x.x-amd64.msi file
	3 - Select the directory to save the program to
	4 - Click Next
	5 - Click Finish when complete

	***********************************************************
	SETUP

	6 - ADDITIONAL REQUIREMENT: MICROSOFT ODBC DRIVER 17 FOR SQL SERVER Needs to be installed on the host computer

	    	LINK: https://www.microsoft.com/en-us/download/details.aspx?id=56567

	
	7 - You also need a service account key file that has access to the BQ project you will be sending your data to (name it bq_creds.json).
		
		REFERENCE: https://cloud.google.com/bigquery/docs/authentication/service-account-file

*************************************************************************************************************************
HOW TO USE:
	1 - Create a SQL folder which will hold your SQL to BQ folders
	2 - In your SQL to BQ Settings folder create a folder for your query
	3 - In this folder you will create 2 files
		- bq_info.txt (BQ Settings file: DO NOT RENAME THIS FILE)

			 ################################################

			     BQ_PROJECT_NAME
        		 DATASET.TABLE_NAME
        		 BQ_TABLE_REPLACE_METHOD (append or replace)

			 example:
			 xpo-data-cloud
			 xpotools.simstest
			 replace

			 ################################################

		- db_shortname-query_name.txt (This contains your query
			viaware_pdatl-query.txt
			# the first portion of this filename (before the hyphen) is a database shortname that is hardcoded 
			  in the python code itself which will set the server and database
			# the second portion of this filename

	4 - Create a folder for your .bat files
	5 - In that folder create a .bat file that contains the format below

			#######################################################################
		
			start /PATH TO/sql_to_bq.exe "---PATH TO YOUR QUERY FOLDER---"

			#######################################################################

	6 - Run the file and you are finished
	
	NOTE: If you are experiencing issues with your table not being created in BQ you can troubleshoot by going to the 
	      file below to see what the error is

		LOG - \PATH TO\SQL_to_BQ\1build\log\log.txt
		

*************************************************************************************************************************