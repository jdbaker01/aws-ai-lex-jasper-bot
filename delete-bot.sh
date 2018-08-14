#!/bin/bash

#
# Deletes the bot, intents, and custom slot types
# in reverse order of build: first the bot, then
# the intents, then the slot types.
#

BOT="JasperX"
INTENTS="CompareX CountX GoodByeX HelloX RefreshX ResetX TopX"
SLOTS="CompareX CountX PrepositionX ResetX TicketsSoldX TopX VersusX cat_descX dimensionsX event_nameX"
LAMBDA="JasperX"
SLEEP=2

# delete bot aliases -- fix this
## echo "Deleting Bot alias: jasper_bot"
## aws lex-models delete-bot-alias --name jasper_test --bot-name $BOT

# delete the bot if it exists
echo -n "Checking for existing bot $BOT... " 
if aws lex-models get-bot --name $BOT --version-or-alias '$LATEST' >/dev/null 2>&1
then 
    sleep $SLEEP
    echo "deleting."
    aws lex-models delete-bot --name $BOT
    sleep $SLEEP
else
    echo "not found."
fi

# delete the intents
for i in $INTENTS
do
    echo -n "Checking for existing intent $i... "
    if aws lex-models get-intent --name $i --intent-version '$LATEST' >/dev/null 2>&1
    then 
        sleep $SLEEP
        echo "deleting."
        aws lex-models delete-intent --name $i
        sleep $SLEEP
    else
        echo "not found."
    fi
done

# delete the custom slot types
for i in $SLOTS
do
    echo -n "Checking for existing slot type $i... "
    if aws lex-models get-slot-type --name $i --slot-type-version '$LATEST' >/dev/null 2>&1
    then 
        sleep $SLEEP
        echo "deleting."
        aws lex-models delete-slot-type --name $i
        sleep $SLEEP
    else
        echo "not found."
    fi
done
 
# delete the lambda function
echo -n "Checking for existing Lambda function $LAMBDA... "
if aws lambda get-function --function-name $LAMBDA >/dev/null 2>&1
then 
    sleep $SLEEP
    echo "deleting."
    aws lambda delete-function --function-name $LAMBDA
    sleep $SLEEP
else
    echo "not found."
fi
