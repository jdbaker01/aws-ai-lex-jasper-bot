# aws-ai-lex-jasper-bot
Jasper - a conversational business intelligence chatbot

Jasper is a business intelligence chatbot that can respond to user questions about data in a database, by converting those questions into backend database queries, and transforming the result sets into natural language responses.  This blog post will show how Jasper has been integrated with a typical relational database intended for business intelligence and reporting applications.

TODO: 
1. Default region environment variable?
2. Parameterize the URI for the Lambda handler in the intents
3. Fix key error bug in lambda line 421
4. IN refresh, put_intent.  Was not building, checksum error on CompareX intent.  Working now, test more.  Look for the CLEANUP lines in refresh_intent_handler and clean them up
5. build-db.sh - incorporate it into the CodePipeline as a separate CodeBuild project, triggered from same repo
6. playing with branches - be careful!
