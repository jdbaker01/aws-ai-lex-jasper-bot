
#
# Builds the bot, intents, and custom slot types
#

BOT="JasperX"
INTENTS="CompareX CountX GoodByeX HelloX RefreshX ResetX TopX"
# SLOTS="CompareX CountX PrepositionX ResetX TicketsSoldX TopX VersusX cat_descX dimensionsX event_nameX"
SLOTS="CompareX CountX"

# delete the custom slot types
for i in $SLOTS
do
	echo "Creating Slot Type: $i"
	aws lex-models put-slot-type --name $i --cli-input-json file://slots/$i.json
done

# delete the intents
# for i in $INTENTS
# do
# 	if aws lex-models get-intent --name $i --intent-version '$LATEST' # >>/dev/null 2>&1
#    	then echo "Deleting Intent: $i"; aws lex-models delete-intent --name $i
# 	fi
# done
  
# delete the bot if it exists
# if aws lex-models get-bot --name $BOT --version-or-alias '$LATEST' # >>/dev/null 2>&1
# then echo "Deleting Bot: $BOT"; aws lex-models delete-bot $BOT
# fi

