#!/bin/bash

#
# Copies the sample data and creates the Athena database
#

#
# Environment variables to be set in the CodeBuild project
#
# $ATHENA_DB    		Name of the Athena database
# $ATHENA_BUCKET		Name of the S3 bucket where the data is stored
# $ATHENA_BUCKET_REGION		Region for the S3 bucket where the data is stored
# $ATHENA_DB_DESCRIPTION	Description for the Athena database
# $SOURCE_DATA			Source S3 bucket for the data
# $SOURCE_DATA_REGION		Source S3 bucket region
#

echo "Starting build-db.sh"
echo '$ATHENA_DB' " = $ATHENA_DB"
echo '$ATHENA_BUCKET' " = $ATHENA_BUCKET"
echo '$ATHENA_BUCKET_REGION' " = $ATHENA_BUCKET_REGION"
echo '$ATHENA_DB_DESCRIPTION' " = $ATHENA_DB_DESCRIPTION"
echo '$SOURCE_DATA' " = $SOURCE_DATA"
echo '$SOURCE_DATA_REGION' " = $SOURCE_DATA_REGION"
echo

#
# Create S3 bucket for Athena data
#
if aws s3 ls $ATHENA_BUCKET >/dev/null
then echo "S3 bucket $ATHENA_BUCKET already exists."
else echo "Creating S3 bucket $ATHENA_BUCKET."; aws a3 mb $ATHENA_BUCKET
fi

#
# Delete TICKTI database if it exists
#
if aws glue get-database --name $ATHENA_DB >xxx 2>&1
then echo "Deleting Athena database $ATHENA_DB"; aws glue delete-database --name $ATHENA_DB >/dev/null
fi

#
# Create TICKIT database
#
echo "Creating Athena database $ATHENA_DB"
aws glue create-database --database-input "Name=$ATHENA_DB,Description=$ATHENA_DB_DESCRIPTION" >/dev/null

#
# Create TICKIT users table in Athena
#
echo "Copying user data to S3..."
aws s3 cp $SOURCE_DATA/users/allusers_pipe.txt $ATHENA_BUCKET/users/allusers_pipe.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

echo "Creating users table..."
aws athena start-query-execution --query-string "create external table users (user_id INT, username STRING, firstname STRING, lastname STRING, city STRING, state STRING, email STRING, phone STRING, like_sports BOOLEAN, liketheatre BOOLEAN, likeconcerts BOOLEAN, likejazz BOOLEAN, likeclassical BOOLEAN, likeopera BOOLEAN, likerock BOOLEAN, likevegas BOOLEAN, likebroadway BOOLEAN, likemusicals BOOLEAN) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '$ATHENA_BUCKET/users';" --query-execution-context "Database=$ATHENA_DB" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/" >/dev/null

# REMOVE THESE
# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

#
# Create TICKIT venue table in Athena
#
echo "Copying venue data to S3..."
aws s3 cp $SOURCE_DATA/venue/venue_pipe.txt $ATHENA_BUCKET/venue/venue_pipe.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

echo "Creating venue table..."
aws athena start-query-execution --query-string "create external table venue (venue_id INT, venue_name STRING, venue_city STRING, venue_state STRING, venue_seats INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '$ATHENA_BUCKET/venue';" --query-execution-context "Database=$ATHENA_DB" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/" >/dev/null

# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

#
# Create TICKIT category table in Athena
#
echo "Copying category data to S3..."
aws s3 cp $SOURCE_DATA/category/category_pipe.txt $ATHENA_BUCKET/category/category_pipe.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

echo "Creating category table..."
aws athena start-query-execution --query-string "create external table category (cat_id INT, cat_group STRING, cat_name STRING, cat_desc STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '$ATHENA_BUCKET/category';" --query-execution-context "Database=$ATHENA_DB" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/" >/dev/null

# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

#
# Create TICKIT date table in Athena
#
echo "Copying date dimension data to S3..."
aws s3 cp $SOURCE_DATA/date/date2008_pipe.txt $ATHENA_BUCKET/date/date2008_pipe.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

echo "Creating date_dim table..."
aws athena start-query-execution --query-string "create external table date_dim (date_id INT, cal_date DATE, day STRING, week STRING, month STRING, quarter STRING, year INT, holiday BOOLEAN) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '$ATHENA_BUCKET/date';" --query-execution-context "Database=$ATHENA_DB" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/" >/dev/null

# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

#
# Create TICKIT event table in Athena
#
echo "Copying event data to S3..."
aws s3 cp $SOURCE_DATA/event/allevents_pipe.txt $ATHENA_BUCKET/event/allevents_pipe.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

echo "Creating event table..."
aws athena start-query-execution --query-string "create external table event (event_id INT, venue_id INT, cat_id INT, date_id INT, event_name STRING, start_time TIMESTAMP) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '$ATHENA_BUCKET/event';" --query-execution-context "Database=$ATHENA_DB" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/" >/dev/null

# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

#
# Create TICKIT listing table in Athena
#
echo "Copying listing data to S3..."
aws s3 cp $SOURCE_DATA/listing/listings_pipe.txt $ATHENA_BUCKET/listing/listings_pipe.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

echo "Creating listing table..."
aws athena start-query-execution --query-string "create external table listing (list_id INT, seller_id INT, event_id INT, date_id INT, qty INT, price DECIMAL(8,2), total DECIMAL(8,2), listing_time TIMESTAMP) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '$ATHENA_BUCKET/listing';" --query-execution-context "Database=$ATHENA_DB" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/" >/dev/null

# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

#
# Create TICKIT sales table in Athena
#
echo "Copying sales data to S3..."
aws s3 cp $SOURCE_DATA/sales/sales_ts.000 $ATHENA_BUCKET/sales/sales_tab.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

echo "Creating sales table..."
aws athena start-query-execution --query-string "create external table sales (sales_id INT, list_id INT, seller_id INT, buyer_id INT, event_id INT, date_id INT, qty INT, amount DECIMAL(8,2), commission DECIMAL(8,2), sale_time TIMESTAMP) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '$ATHENA_BUCKET/sales';" --query-execution-context "Database=$ATHENA_DB" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/" >/dev/null

# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

