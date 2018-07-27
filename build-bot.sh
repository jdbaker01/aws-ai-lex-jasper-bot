
#
# Builds the bot, intents, and custom slot types
#

BOT="JasperX"
INTENTS="CompareX CountX GoodByeX HelloX RefreshX ResetX TopX"
SLOTS="CompareX CountX PrepositionX ResetX TicketsSoldX TopX VersusX cat_descX dimensionsX event_nameX"

# deploy the Lambda intent handler
aws lambda create-function --function-name JasperX --description 'JasperX Bot - Tickit database' --timeout 300 --zip-file fileb://JasperLambda.zip --role arn:aws:iam::687551564203:role/LambdaServiceRoleAthenaS3 --handler lambda_function.lambda_handler --runtime python3.6 

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
aws lex-models put-bot --name $BOT --cli-input-json file://bots/$BOT.json

