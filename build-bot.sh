#!/bin/bash

#
# Builds the bot, intents, and custom slot types
#

#
# Environment variables to be set in the CodeBuild project
#
# $BOT				Name of the Lex bot
# $ALIAS			Name of the published alias for the bot
# $INTENTS      		List of intent names for the bot
# $SLOTS        		List of slot type names for the bot
# $LAMBDA       		Name of the Lambda fulfillment function for the bot
# $LAMBDA_ROLE_ARN     		ARN for the Lambda execution role
# $ATHENA_DB    		Name of the Athena database
# $ATHENA_OUTPUT_LOCATION	Name of the S3 bucket for Athena output
#

# deploy the Lambda intent handler
echo "Creating Lambda handler function: $LAMBDA, Lambda execution role = $LAMBDA_ROLE_ARN"
aws lambda create-function \
    --function-name $LAMBDA \
    --description "$LAMBDA Intent Handler" \
    --timeout 300 \
    --zip-file fileb://JasperLambda.zip \
    --role $LAMBDA_ROLE_ARN \
    --handler lambda_function.lambda_handler \
    --runtime python3.6 \
    --environment "Variables={ATHENA_DB=$ATHENA_DB,ATHENA_OUTPUT_LOCATION=$ATHENA_OUTPUT_LOCATION}" \
    >/dev/null

LAMBDA_ARN=`aws lambda get-function --function-name $LAMBDA | grep 'FunctionArn' | sed 's/.*FunctionArn": "\(.*\)".*/\1/'`
echo "Lambda ARN = \'$LAMBDA_ARN\'"

echo "Adding permission to invoke Lambda handler function $LAMBDA from Amazon Lex"
aws lambda add-permission --function-name $LAMBDA --statement-id chatbot-fulfillment --action "lambda:InvokeFunction" --principal "lex.amazonaws.com" >/dev/null

# build the custom slot types
for i in $SLOTS
do
	echo "Creating slot type: $i"
	aws lex-models put-slot-type --name $i --cli-input-json file://slots/$i.json >/dev/null 
done

# build the intents
for i in $INTENTS
do
	echo "Creating intent: $i"
        # substitute the ARN for the Lambda fulfullment function
        sed "s/{{lambda_arn}}/$LAMBDA_ARN/" intents/$i.json >intents/$i-updated.json
        grep arn intents/$i-updated.json
	aws lex-models put-intent --name $i --cli-input-json file://intents/$i-updated.json >/dev/null 
done

# build the bot 
echo "Creating bot: $BOT"
if aws lex-models put-bot --name $BOT --cli-input-json file://bots/$BOT.json >/dev/null
then echo "Success: $BOT bot build complete."; exit 0
else echo "Error: $BOT bot build failed, check the log for errors"; exit 1
fi

# create bot alias
## echo "Creating bot alias: $ALIAS"
## aws lex-models put-bot-alias --name $ALIAS --bot-name $BOT --bot-version '$LATEST' >/dev/null

# refresh the bot
## echo "Calling refresh intent"
## aws lex-runtime post-text --bot-name $BOT --bot-alias jasper_bot --user-id a_user --input-text "refresh"
