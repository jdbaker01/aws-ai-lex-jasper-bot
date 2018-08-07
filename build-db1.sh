
#
# ---> NOTE: USE ENV VARIABLES FOR DB LOCATIONS <---
#
SOURCE_DATA="s3://awssampledbuswest2/tickit/spectrum"
ATHENA_BUCKET="s3://ai-blogpost-tickit-db-y"
ATHENA_DB_NAME="tickit-y"
ATHENA_DB_DESCRIPTION="Test TICKIT database for Lex business intelligence bot (Jasper)"

#
# Create TICKIT users table in Athena
#
aws s3 cp $SOURCE_DATA/users/allusers_pipe.txt $ATHENA_BUCKET/users/allusers_pipe.txt --recursive --source-region us-west-2 --region us-west-1

