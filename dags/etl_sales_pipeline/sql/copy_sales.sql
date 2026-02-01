COPY sales_clean
FROM 's3:/nag-sales-data-bucket/curated/sales/{{ ds }}/'
IAM_ROLE 'arn:aws:iam::123456789012:role/redshift-role'
FORMAT AS PARQUET;
