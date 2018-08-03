# aws-ai-lex-jasper-bot
Jasper - a conversational business intelligence chatbot

Jasper is a business intelligence chatbot that can respond to user questions about data in a database, by converting those questions into backend database queries, and transforming the result sets into natural language responses.  This blog post will show how Jasper has been integrated with a typical relational database intended for business intelligence and reporting applications.

TODO: 
1. Default region environment variable?
2. Lambda function seems to not be remembering properly - probably because I changed slot/intent names
   - example: refresh_intent_handler, line 767, it is rebuilding Jasper not JasperX
   - Just rewrote the dispatcher
3. Parameterize the URI for the Lambda handler in the intents
4. IN refresh, put_intent.  Was not building, checksum error on CompareX intent.  Working now, test more.  Look for the CLEANUP lines in refresh_intent_handler and clean them up
