COPY sales_clean
FROM 's3://nag-sales-data-bucket/curated/sales/{{ ds }}/'
IAM_ROLE 'arn:aws:iam::467866449044:role/service-role/AmazonRedshift-CommandsAccessRole-20260201T205546'
FORMAT AS PARQUET;
