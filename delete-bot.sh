
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

# delete the bot if it exists
if aws lex-models get-bot --name $BOT --version-or-alias '$LATEST' >/dev/null
then 
    # echo "sleep $SLEEP..."
    sleep $SLEEP
    echo "Deleting Bot: $BOT"
    aws lex-models delete-bot --name $BOT
    # echo "sleep $SLEEP..."
    sleep $SLEEP
fi

# delete the intents
for i in $INTENTS
do
    if aws lex-models get-intent --name $i --intent-version '$LATEST' >/dev/null
    then 
        # echo "sleep $SLEEP..."
        sleep $SLEEP
        echo "Deleting Intent: $i"
        aws lex-models delete-intent --name $i
        # echo "sleep $SLEEP..."
        sleep $SLEEP
    fi
done

# delete the custom slot types
for i in $SLOTS
do
    if aws lex-models get-slot-type --name $i --slot-type-version '$LATEST' >/dev/null
    then 
        # echo "sleep $SLEEP..."
        sleep $SLEEP
        echo "Deleting Slot Type: $i"
        aws lex-models delete-slot-type --name $i
        # echo "sleep $SLEEP..."
        sleep $SLEEP
    fi
done
 
# delete the lambda function
if aws lambda get-function --function-name $LAMBDA >/dev/null
then 
    # echo "sleep $SLEEP..."
    sleep $SLEEP
    echo "Deleting Lambda Function: $LAMBDA"
    aws lambda delete-function --function-name $LAMBDA
    # echo "sleep $SLEEP..."
    sleep $SLEEP
fi
