# etl_customer360-lallith
End-to-end Python ETL pipeline that cleans, validates, and merges CRM, web, and transaction data into a unified Customer 360 view for analytics.
The project entails creating an ETL pipeline for three distinct raw datasets—CRM leads, web activity, and transaction data—that will be combined into Customer 360, a clear, analytics-ready view.

 Data is extracted, cleaned, validated, deduplicated, transformed, and then exported in a Parquet format as a single, cohesive dataset.

 How to Run:
 Requirements
 Installing pandas pyarrow with pip

 Actions to Take:
 Place all of the input files, such as transactions.txt, web_activity.json, and crm_leads.csv, in the same folder as the Python script, etl_customer360.py.

 To execute the script, use:
 etl_customer360.py in Python

 Alternatively, provide the file paths directly:
 etl_customer360.py in Python  --crm crm_leads.csv --web web_activity.json --tx transactions.txt --outdir.

 After that, you'll notice:

 The procedure was successfully finished!
