
#
# ---> NOTE: USE ENV VARIABLES FOR DB LOCATIONS <---
#
SOURCE_DATA="s3://awssampledbuswest2/tickit/spectrum"
SOURCE_DATA_REGION="us-west-2"
ATHENA_BUCKET="s3://ai-blogpost-tickit-db-y"
ATHENA_BUCKET_REGION="us-east-1"
ATHENA_DB_NAME="tickit-z"
ATHENA_DB_DESCRIPTION="Test TICKIT database for Lex business intelligence bot (Jasper)"

#
# Create S3 bucket for Athena data
#
### aws s3 mb $ATHENA_BUCKET


#
# Delete TICKTI database if it exists
#
if aws glue get-database --name $ATHENA_DB_NAME >xxx 2>&1
then echo "Deleting Athena database $ATHENA_DB_NAME"; aws glue delete-database --name $ATHENA_DB_NAME
fi

#
# Create TICKIT database
#
echo "Creating Athena database $ATHENA_DB_NAME"
aws glue create-database --database-input "Name=$ATHENA_DB_NAME,Description=$ATHENA_DB_DESCRIPTION" 

#
# Create TICKIT users table in Athena
#
echo "Creating users table..."
aws s3 cp $SOURCE_DATA/users/allusers_pipe.txt $ATHENA_BUCKET/users/allusers_pipe.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

aws athena start-query-execution --query-string "create external table users (user_id INT, username STRING, firstname STRING, lastname STRING, city STRING, state STRING, email STRING, phone STRING, like_sports BOOLEAN, liketheatre BOOLEAN, likeconcerts BOOLEAN, likejazz BOOLEAN, likeclassical BOOLEAN, likeopera BOOLEAN, likerock BOOLEAN, likevegas BOOLEAN, likebroadway BOOLEAN, likemusicals BOOLEAN) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '$ATHENA_BUCKET/users';" --query-execution-context "Database=$ATHENA_DB_NAME" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/"

# REMOVE THESE
# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

#
# Create TICKIT venue table in Athena
#
echo "Creating venue table..."
aws s3 cp $SOURCE_DATA/venue/venue_pipe.txt $ATHENA_BUCKET/venue/venue_pipe.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

aws athena start-query-execution --query-string "create external table venue (venue_id INT, venue_name STRING, venue_city STRING, venue_state STRING, venue_seats INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '$ATHENA_BUCKET/venue';" --query-execution-context "Database=$ATHENA_DB_NAME" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/"

# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

#
# Create TICKIT category table in Athena
#
echo "Creating category table..."
aws s3 cp $SOURCE_DATA/category/category_pipe.txt $ATHENA_BUCKET/category/category_pipe.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

aws athena start-query-execution --query-string "create external table category (cat_id INT, cat_group STRING, cat_name STRING, cat_desc STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '$ATHENA_BUCKET/category';" --query-execution-context "Database=$ATHENA_DB_NAME" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/"

# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

#
# Create TICKIT date table in Athena
#
echo "Creating date_dim table..."
aws s3 cp $SOURCE_DATA/date/date2008_pipe.txt $ATHENA_BUCKET/date/date2008_pipe.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

aws athena start-query-execution --query-string "create external table date_dim (date_id INT, cal_date DATE, day STRING, week STRING, month STRING, quarter STRING, year INT, holiday BOOLEAN) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '$ATHENA_BUCKET/date';" --query-execution-context "Database=$ATHENA_DB_NAME" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/"

# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

#
# Create TICKIT event table in Athena
#
echo "Creating event table..."
aws s3 cp $SOURCE_DATA/event/allevents_pipe.txt $ATHENA_BUCKET/event/allevents_pipe.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

aws athena start-query-execution --query-string "create external table event (event_id INT, venue_id INT, cat_id INT, date_id INT, event_name STRING, start_time TIMESTAMP) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '$ATHENA_BUCKET/event';" --query-execution-context "Database=$ATHENA_DB_NAME" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/"

# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

#
# Create TICKIT listing table in Athena
#
echo "Creating listing table..."
aws s3 cp $SOURCE_DATA/listing/listings_pipe.txt $ATHENA_BUCKET/listing/listings_pipe.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

aws athena start-query-execution --query-string "create external table listing (list_id INT, seller_id INT, event_id INT, date_id INT, qty INT, price DECIMAL(8,2), total DECIMAL(8,2), listing_time TIMESTAMP) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LOCATION '$ATHENA_BUCKET/listing';" --query-execution-context "Database=$ATHENA_DB_NAME" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/"

# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

#
# Create TICKIT sales table in Athena
#
echo "Creating sales table..."
aws s3 cp $SOURCE_DATA/sales/sales_ts.000 $ATHENA_BUCKET/sales/sales_tab.txt --recursive --source-region $SOURCE_DATA_REGION --region $ATHENA_BUCKET_REGION

aws athena start-query-execution --query-string "create external table sales (sales_id INT, list_id INT, seller_id INT, buyer_id INT, event_id INT, date_id INT, qty INT, amount DECIMAL(8,2), commission DECIMAL(8,2), sale_time TIMESTAMP) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LOCATION '$ATHENA_BUCKET/sales';" --query-execution-context "Database=$ATHENA_DB_NAME" --result-configuration "OutputLocation=$ATHENA_BUCKET/output/"

# aws athena get-query-execution --query-execution-id <QueryExecutionId>   # <-- if you want to check the status, substitute in your QueryExecutionID

