
#
# Builds the bot, intents, and custom slot types
#

BOT="JasperX"
INTENTS="CompareX CountX GoodByeX HelloX RefreshX ResetX TopX"
SLOTS="CompareX CountX PrepositionX ResetX TicketsSoldX TopX VersusX cat_descX dimensionsX event_nameX"
LAMBDA="JasperX"
ATHENA_DB="tickit"
ATHENA_OUTPUT_LOCATION="s3://ai-aod-demo-bryost-athena-output"

# deploy the Lambda intent handler
echo "Creating Lambda handler function: $LAMBDA"
aws lambda create-function --function-name $LAMBDA --description "$LAMBDA Intent Handler" --timeout 300 --zip-file fileb://JasperLambda.zip --role arn:aws:iam::687551564203:role/LambdaServiceRoleAthenaS3 --handler lambda_function.lambda_handler --runtime python3.6 --environment "Variables={ATHENA_DB=$ATHENA_DB,ATHENA_OUTPUT_LOCATION=$ATHENA_OUTPUT_LOCATION}" >/dev/null

echo "Adding permission to invoke Lambda handler function $LAMBDA from Amazon Lex"
aws lambda add-permission --function-name $LAMBDA --statement-id chatbot-fulfillment --action "lambda:InvokeFunction" --principal "lex.amazonaws.com" >/dev/null

# build the custom slot types
for i in $SLOTS
do
	echo "Creating Slot Type: $i"
	aws lex-models put-slot-type --name $i --cli-input-json file://slots/$i.json >/dev/null 
done

# build the intents
for i in $INTENTS
do
	echo "Creating Intent: $i"
	aws lex-models put-intent --name $i --cli-input-json file://intents/$i.json >/dev/null 
done

# build the bot 
echo "Creating Bot: $BOT"
if aws lex-models put-bot --name $BOT --cli-input-json file://bots/$BOT.json >/dev/null
then echo "Success: $BOT bot build complete."; exit 0
else echo "Error: $BOT bot build failed, check the log for errors"; exit 1
fi

