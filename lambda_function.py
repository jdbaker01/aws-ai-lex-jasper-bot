import boto3
import botocore
import time
import logging
import json
import pprint
import os

#
# See additional configuration parameters at bottom 
#

ATHENA_DB = ''                  # set in environment variable
ATHENA_OUTPUT_LOCATION = ''     # set in environment variable

ORIGINAL_VALUE = 0
TOP_RESOLUTION = 1

SLOT_CONFIG = {
    'event_name':       {'type': TOP_RESOLUTION, 'remember': True,  'error': 'I couldn\'t find an event called "{}".'},
    'event_month':      {'type': ORIGINAL_VALUE, 'remember': True},
    'venue_name':       {'type': ORIGINAL_VALUE, 'remember': True},
    'venue_city':       {'type': ORIGINAL_VALUE, 'remember': True},
    'venue_state':      {'type': ORIGINAL_VALUE, 'remember': True},
    'cat_desc':         {'type': TOP_RESOLUTION, 'remember': True,  'error': 'I couldn\'t find a category called "{}".'},
    'count':            {'type': ORIGINAL_VALUE, 'remember': True},
    'dimension':        {'type': ORIGINAL_VALUE, 'remember': True},
    'one_event':        {'type': TOP_RESOLUTION, 'remember': False, 'error': 'I couldn\'t find an event called "{}".'},
    'another_event':    {'type': TOP_RESOLUTION, 'remember': False, 'error': 'I couldn\'t find an event called "{}".'},
    'one_venue':        {'type': ORIGINAL_VALUE, 'remember': False},
    'another_venue':    {'type': ORIGINAL_VALUE, 'remember': False},
    'one_month':        {'type': ORIGINAL_VALUE, 'remember': False},
    'another_month':    {'type': ORIGINAL_VALUE, 'remember': False},
    'one_city':         {'type': ORIGINAL_VALUE, 'remember': False},
    'another_city':     {'type': ORIGINAL_VALUE, 'remember': False},
    'one_state':        {'type': ORIGINAL_VALUE, 'remember': False},
    'another_state':    {'type': ORIGINAL_VALUE, 'remember': False},
    'one_category':     {'type': TOP_RESOLUTION, 'remember': False,  'error': 'I couldn\'t find a category called "{}".'},
    'another_category': {'type': TOP_RESOLUTION, 'remember': False,  'error': 'I couldn\'t find a category called "{}".'}
}

DIMENSIONS = {
    'events':     {'slot': 'event_name',  'column': 'e.event_name',  'singular': 'event'},
    'months':     {'slot': 'event_month', 'column': 'd.month',       'singular': 'month'},
    'venues':     {'slot': 'venue_name',  'column': 'v.venue_name',  'singular': 'venue'},
    'cities':     {'slot': 'venue_city',  'column': 'v.venue_city',  'singular': 'city'},
    'states':     {'slot': 'venue_state', 'column': 'v.venue_state', 'singular': 'state'},
    'categories': {'slot': 'cat_desc',    'column': 'c.cat_desc',    'singular': 'category'}
}

COMPARE_CONFIG = {
    'events':     {'1st': 'one_event',    '2nd': 'another_event',    'error': 'Sorry, try "Compare sales for Event 1 versus Event 2'},
    'months':     {'1st': 'one_month',    '2nd': 'another_month',    'error': 'Sorry, try "Compare sales for Month 1 versus Month 2'},
    'venues':     {'1st': 'one_venue',    '2nd': 'another_venue',    'error': 'Sorry, try "Compare sales for Venue 1 versus Venue 2'},
    'cities':     {'1st': 'one_city',     '2nd': 'another_city',     'error': 'Sorry, try "Compare sales for City 1 versus City 2'},
    'states':     {'1st': 'one_state',    '2nd': 'another_state',    'error': 'Sorry, try "Compare sales for State 1 versus State 2'},
    'categories': {'1st': 'one_category', '2nd': 'another_category', 'error': 'Sorry, try "Compare sales for Category 1 versus Category 2'}
}

# SELECT statement for Count query
COUNT_SELECT = "SELECT SUM(s.qty) FROM sales s, event e, venue v, category c, date_dim d "
COUNT_JOIN = " WHERE e.event_id = s.event_id AND v.venue_id = e.venue_id AND c.cat_id = e.cat_id AND d.date_id = e.date_id "
COUNT_WHERE = " AND LOWER({}) LIKE LOWER('%{}%') "   
COUNT_PHRASE = 'tickets sold'

# SELECT statement for Compare query
COMPARE_SELECT = "SELECT {}, SUM(s.amount) ticket_sales  FROM sales s, event e, venue v, category c, date_dim d "
COMPARE_JOIN = " WHERE e.event_id = s.event_id AND v.venue_id = e.venue_id AND c.cat_id = e.cat_id AND d.date_id = e.date_id "
COMPARE_WHERE = " AND LOWER({}) LIKE LOWER('%{}%') "  
COMPARE_ORDERBY = " GROUP BY {} ORDER BY ticket_sales DESC "

# SELECT statement for Top query
TOP_SELECT  = "SELECT {}, SUM(s.amount) ticket_sales FROM sales s, event e, venue v, category c, date_dim d  "
TOP_JOIN    = " WHERE e.event_id = s.event_id AND v.venue_id = e.venue_id AND c.cat_id = e.cat_id AND d.date_id = e.date_id "
TOP_WHERE   = " AND LOWER({}) LIKE LOWER('%{}%') " 
TOP_ORDERBY = " GROUP BY {} ORDER BY ticket_sales desc" 
TOP_DEFAULT_COUNT = '5'

session_attributes = {}         # session attributes, used to retain conversational state

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

class SlotError(Exception):
    pass


def lambda_handler(event, context):
    ## ALEXA session_attributes = event['sessionAttributes']
    session_attributes = {}

    logger.debug('<<Jasper>> Lex event info = ' + json.dumps(event))

    config_error = get_jasper_config()
    if config_error is not None:
        return close(session_attributes, 'Fulfilled',
            {'contentType': 'PlainText', 'content': config_error})   
    else:
        return dispatch(event)


def dispatch(intent_request):
    ## ALEXA logger.debug('<<Jasper>> dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    if intent_name is not None:
        if INTENT_CONFIG.get(intent_name, False):
            return INTENT_CONFIG[intent_name]['handler'](intent_request)    # dispatch to the event handler
        else:
            return close(session_attributes, 'Fulfilled',
                {'contentType': 'PlainText', 'content': 'Sorry, I don\'t support the intent called "' + intent_name + '".'})
    else:
        return close(session_attributes, 'Fulfilled',
            {'contentType': 'PlainText', 'content': 'Missing intent name.'})


def hello_intent_handler(intent_request):
    session_attributes['resetCount'] = '0'
    session_attributes['finishedCount'] = '0'

    askCount = increment_counter(session_attributes, 'greetingCount')
    
    # build response string
    if askCount == 1: response_string = 'Jasper here, how can I help?'
    elif askCount == 2: response_string = 'I\'m here'
    elif askCount == 3: response_string = 'I\'m listening'
    elif askCount == 4: response_string = 'Yes?'
    elif askCount == 5: response_string = 'Really?'
    else: response_string = 'Ok'

    return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': response_string})   


def reset_intent_handler(intent_request):
    session_attributes['greetingCount'] = '1'
    session_attributes['finishedCount'] = '0'
    
    # Retrieve "remembered" slot values from session attributes
    slot_values = get_remembered_slot_values(None, session_attributes)

    dimensions_reset = ''

    # Retrieve slot values from the current request to see what needs to be reset
    slots_to_reset = get_slot_values(None, intent_request)

    # check to see if any remembered slots need forgetting
    for key,config in SLOT_CONFIG.items():
        if key == 'dimension':    # see below
            continue
        if config.get('remember', False):
            if slots_to_reset.get(key):        # asking to reset venue_city: los angeles for example
                if slot_values.get(key):
                    value = post_process_dimension_output(key, slot_values.get(key))
                    dimensions_reset += ' {}'.format(value.title())
                    logger.debug('<<Jasper>> reset_intent_handler() - forgetting slot %s value %s', key, slot_values[key])
                    slot_values[key] = None
                else:
                    message = "I wasn't remembering {} - {} anyway.".format(key, slots_to_reset.get(key))
                    return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText', 'content': message})

    # check for special case, where the ask is to forget the dimension by name
    dimension = slots_to_reset.get('dimension')
    if dimension and DIMENSIONS.get(dimension):
        slot_key = DIMENSIONS[dimension].get('slot')
        if slot_values.get(slot_key):
            logger.debug('<<Jasper>> reset_intent_handler() - forgetting %s (%s)', dimension, slot_values[slot_key])
            value = post_process_dimension_output(dimension, slot_values[slot_key])
            dimensions_reset += ' {}'.format(value).title()
            logger.debug('<<Jasper>> reset_intent_handler() - forgetting dimension %s slot_key %s value %s', dimension, slot_key, slot_values[slot_key])
            slot_values[slot_key] = None

    if dimensions_reset == '':
        slot_values = {key: None for key in SLOT_CONFIG}
        dimensions_reset = 'everything'
    
    remember_slot_values(slot_values, session_attributes)

    response_string = 'OK, I have reset ' + dimensions_reset + '.'

    return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': response_string})   


def goodbye_intent_handler(intent_request):
    session_attributes['greetingCount'] = '0'
    session_attributes['resetCount'] = '0'
    session_attributes['queryAttributes'] = None

    askCount = increment_counter(session_attributes, 'finishedCount')

    # build response string
    if askCount == 1: response_string = 'Nice chatting with you.  Talk to you later!'
    elif askCount == 2: response_string = 'Bye now!'
    elif askCount == 3: response_string = 'Hope I was able to help!'
    elif askCount == 4: response_string = 'See ya!'
    elif askCount == 5: response_string = 'Really?'
    else: response_string = 'Ok'

    return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': response_string})   


def switch_intent_handler(intent_request):
    session_attributes['greetingCount'] = '0'
    session_attributes['resetCount'] = '0'
    session_attributes['finishedCount'] = '0'
    session_attributes['queryAttributes'] = None

    slot_values = get_slot_values(None, intent_request)
'''
    try:
        slot_values = get_slot_values(slot_values, intent_request)
    except SlotError as err:
        return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': str(err)})   
'''
    response_string = 'SWITCH: slot_values = {}'.format(slot_values)

    return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': response_string})   


def count_intent_handler(intent_request):
    method_start = time.perf_counter()
    
    athena = boto3.client('athena')
    
    session_attributes['greetingCount'] = '1'
    session_attributes['resetCount'] = '0'
    session_attributes['finishedCount'] = '0'

    # Retrieve slot values from the current request
    slot_values = session_attributes.get('slot_values')

    try:
        slot_values = get_slot_values(slot_values, intent_request)
    except SlotError as err:
        return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': str(err)})   
    
    logger.debug('<<Jasper>> "count_intent_handler(): slot_values: %s', slot_values)

    # Retrieve "remembered" slot values from session attributes
    slot_values = get_remembered_slot_values(slot_values, session_attributes)
    logger.debug('<<Jasper>> "count_intent_handler(): slot_values afer get_remembered_slot_values: %s', slot_values)

    # Remember updated slot values
    remember_slot_values(slot_values, session_attributes)
    
    # build and execute query
    select_clause = COUNT_SELECT
    where_clause = COUNT_JOIN
    for dimension in DIMENSIONS:
        slot_key = DIMENSIONS.get(dimension).get('slot')
        if slot_values[slot_key] is not None:
            value = pre_process_query_value(slot_key, slot_values[slot_key])
            where_clause += COUNT_WHERE.format(DIMENSIONS.get(dimension).get('column'), value)
    
    query_string = select_clause + where_clause
    
    response = execute_athena_query(query_string)

    result = response['ResultSet']['Rows'][1]['Data'][0]
    if result:
        count = result['VarCharValue']
    else:
        count = 0

    logger.debug('<<Jasper>> "Count value is: %s' % count) 

    # build response string
    if count == 0:
        response_string = 'There were no {}'.format(COUNT_PHRASE)
    else:
        response_string = 'There were {} {}'.format(count, COUNT_PHRASE)

    # add the English versions of the WHERE clauses
    for dimension in DIMENSIONS:
        slot_key = DIMENSIONS[dimension].get('slot')
        logger.debug('<<Jasper>> pre top5_formatter[%s] = %s', slot_key, slot_values.get(slot_key))
        if slot_values.get(slot_key) is not None:
            # the DIMENSION_FORMATTERS perform a post-process functions and then format the output
            # Example:  {... 'venue_state': {'format': ' in the state of {}',  'function': get_state_name}, ...}
            if DIMENSION_FORMATTERS.get(slot_key) is not None:
                output_text = DIMENSION_FORMATTERS[slot_key]['function'](slot_values.get(slot_key))
                response_string += ' ' + DIMENSION_FORMATTERS[slot_key]['format'].lower().format(output_text)
                logger.debug('<<Jasper>> dimension_formatter[%s] = %s', slot_key, output_text)

    response_string += '.'

    return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': response_string})   


def compare_intent_handler(intent_request):
    method_start = time.perf_counter()
    
    athena = boto3.client('athena')
    
    session_attributes['greetingCount'] = '1'
    session_attributes['resetCount'] = '0'
    session_attributes['finishedCount'] = '0'
    
    # Retrieve slot values from the current request
    slot_values = session_attributes.get('slot_values')
    
    try:
        slot_values = get_slot_values(slot_values, intent_request)
    except SlotError as err:
        return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': str(err)})   
        
    logger.debug('<<Jasper>> "count_intent_handler(): slot_values: %s', slot_values)

    # Retrieve "remembered" slot values from session attributes
    slot_values = get_remembered_slot_values(slot_values, session_attributes)
    logger.debug('<<Jasper>> "count_intent_handler(): slot_values afer get_remembered_slot_values: %s', slot_values)

    # Remember updated slot values
    remember_slot_values(slot_values, session_attributes)
    
    for key,config in COMPARE_CONFIG.items():
        if slot_values.get(config['1st']):
            if slot_values.get(config['2nd']) is None:
                return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText', 'content': config['error'] })
            
            slot_values['dimension'] = key
            slot_values[DIMENSIONS[key]['slot']] = None
            
            the_1st_dimension_value = slot_values[config['1st']].lower()
            the_2nd_dimension_value = slot_values[config['2nd']].lower()

            break

    # TODO: check for no dimension picked
    # TODO: allow for refining prior comparisons just adding WHERE's - remember prior one vs another
    # Check for minimum required slot values

    """
    Example Query:
        SELECT v.venue_city, SUM(s.amount) ticket_sales
          FROM sales s, event e, venue v, category c, date_dim d
         WHERE e.event_id = s.event_id 
           AND v.venue_id = e.venue_id
           AND c.cat_id = e.cat_id
           AND d.date_id = e.date_id
           AND (LOWER(v.venue_city) LIKE LOWER('%Boston%') OR LOWER(v.venue_city) LIKE LOWER('%New York City%'))
           AND LOWER(e.event_name) LIKE LOWER('%electra%')
         GROUP BY v.venue_city
         ORDER BY ticket_sales desc         
    """

    # TODO: replace this whole thing with two independent queries, in case the
    #       user specifies a partial venue name for example, the LIKE SQL will
    #       work but the value will be different so you can't look it up in the
    #       result_set dict with the inputted key
    
    # Build and execute query
    select_clause = COMPARE_SELECT.format(DIMENSIONS[slot_values['dimension']]['column'])
    where_clause = COMPARE_JOIN

    the_1st_dimension_value = pre_process_query_value(DIMENSIONS[key]['slot'], the_1st_dimension_value)
    the_2nd_dimension_value = pre_process_query_value(DIMENSIONS[key]['slot'], the_2nd_dimension_value)
    where_clause += "   AND (LOWER(" + DIMENSIONS[slot_values['dimension']]['column'] + ") LIKE LOWER('%" + the_1st_dimension_value + "%') OR "
    where_clause +=         "LOWER(" + DIMENSIONS[slot_values['dimension']]['column'] + ") LIKE LOWER('%" + the_2nd_dimension_value + "%')) " 

    logger.debug('<<Jasper>> compare_sales_intent_request - building WHERE clause') 
    for dimension in DIMENSIONS:
        slot_key = DIMENSIONS.get(dimension).get('slot')
        if slot_values[slot_key] is not None:
            logger.debug('<<Jasper>> compare_sales_intent_request - calling pre_process_query_value(%s, %s)', 
                         slot_key, slot_values[slot_key])  
            value = pre_process_query_value(slot_key, slot_values[slot_key])
            where_clause += COMPARE_WHERE.format(DIMENSIONS.get(dimension).get('column'), value)

    order_by_group_by = COMPARE_ORDERBY.format(DIMENSIONS[slot_values['dimension']]['column'])

    query_string = select_clause + where_clause + order_by_group_by
    
    logger.debug('<<Jasper>> Athena Query String = ' + query_string)  
    
    response = execute_athena_query(query_string)

    # Build response string
    response_string = ''
    result_count = len(response['ResultSet']['Rows']) - 1

    # add the English versions of the WHERE clauses
    counter = 0
    for dimension in DIMENSIONS:
        slot_key = DIMENSIONS[dimension].get('slot')
        logger.debug('<<Jasper>> pre compare_sale_formatter[%s] = %s', slot_key, slot_values.get(slot_key))
        if slot_values.get(slot_key) is not None:
            # the DIMENSION_FORMATTERS perform a post-process function and then format the output
            # Example:  {... 'venue_state': {'format': ' in the state of {}',  'function': get_state_name}, ...}
            if DIMENSION_FORMATTERS.get(slot_key) is not None:
                output_text = DIMENSION_FORMATTERS[slot_key]['function'](slot_values.get(slot_key))
                if counter == 0:
                    response_string += DIMENSION_FORMATTERS[slot_key]['format'].format(output_text)
                else:
                    response_string += ', ' + DIMENSION_FORMATTERS[slot_key]['format'].lower().format(output_text)
                counter += 1
                logger.debug('<<Jasper>> compare_sales_formatter[%s] = %s', slot_key, output_text)

    if (result_count == 0):
        if len(response_string) > 0:
            response_string += ', '
        response_string += "I didn't find any results for the " + slot_values['dimension']
        response_string += " " + post_process_dimension_output(key, the_1st_dimension_value)
        response_string += " and " + post_process_dimension_output(key, the_2nd_dimension_value) + "."

    elif (result_count == 1):
        if len(response_string) > 0:
            response_string += ', there '
        else:
            response_string += 'There '
        response_string += 'is only one ' + DIMENSIONS[slot_values['dimension']]['singular'] + '.'
        
    elif (result_count == 2):
        # put the results into a dict for easier reference by name
        result_set = {}
        result_set.update( { response['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'].lower() : [
                             response['ResultSet']['Rows'][1]['Data'][0]['VarCharValue'],  
                             float(response['ResultSet']['Rows'][1]['Data'][1]['VarCharValue']) ] } )
        result_set.update( { response['ResultSet']['Rows'][2]['Data'][0]['VarCharValue'].lower() : [
                             response['ResultSet']['Rows'][2]['Data'][0]['VarCharValue'],  
                             float(response['ResultSet']['Rows'][2]['Data'][1]['VarCharValue']) ] } )

        # TODO: fix this.  Test case: top cities for chicago, compare sales for phoenix and new york (instead of new york city)
        # TODO: problem - if you spell an event name incorrectly it may find it in the SQL
        # query, but it will create an error in the result_set[] lookup

        logger.debug('<<Jasper>> compare_intent_handler - result_set = %s', result_set) 

        the_1st_dimension_string = result_set[the_1st_dimension_value][0]
        the_1st_dimension_string = post_process_dimension_output(key, the_1st_dimension_string)
        the_2nd_dimension_string = result_set[the_2nd_dimension_value][0]
        the_2nd_dimension_string = post_process_dimension_output(key, the_2nd_dimension_string)

        if len(response_string) == 0:
            response_string = 'Sales for ' + the_1st_dimension_string + ' were '
        else:
            response_string += ', sales for ' + the_1st_dimension_string + ' were '

        the_1st_amount = result_set[the_1st_dimension_value][1]
        the_2nd_amount = result_set[the_2nd_dimension_value][1]
        
        the_1st_amount_formatted = '{:,.0f}'.format(the_1st_amount)
        the_2nd_amount_formatted = '{:,.0f}'.format(the_2nd_amount)
        
        if (the_1st_amount == the_2nd_amount):
            response_string += 'the same as for ' + the_2nd_dimension_string + ', $' + the_2nd_amount_formatted
        else:
            if (the_1st_amount < the_2nd_amount):
                percent_different = (the_1st_amount - the_2nd_amount) / the_2nd_amount * -1
                higher_or_lower = 'lower'
            else:
                percent_different = (the_1st_amount - the_2nd_amount) / the_2nd_amount
                higher_or_lower = 'higher'

            response_string += '{:.0%}'.format(percent_different) + ' ' + higher_or_lower + ' than for ' + the_2nd_dimension_string
            response_string += ': $' + the_1st_amount_formatted + ' as opposed to $' + the_2nd_amount_formatted + '.'

    else:  # >2, should not occur
        response_string = 'I seem to have a problem, I got back ' + str(result_count) + ' ' + dimension + '.'
    
    logger.debug('<<Jasper>> response_string = ' + response_string) 

    method_duration = time.perf_counter() - method_start
    method_duration_string = 'method time = %.0f' % (method_duration * 1000) + ' ms'
    logger.debug('<<Jasper>> "Method duration is: ' + method_duration_string) 

    return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': response_string})   


def top_intent_handler(intent_request):
    # TODO: make this a decorator
    method_start = time.perf_counter()
    
    session_attributes['greetingCount'] = '1'
    session_attributes['resetCount'] = '0'
    session_attributes['finishedCount'] = '0'

    # Retrieve slot values from the current request
    slot_values = session_attributes.get('slot_values')

    try:
        slot_values = get_slot_values(slot_values, intent_request)
    except SlotError as err:
        return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': str(err)})   

    logger.debug('<<Jasper>> "top_intent_handler(): slot_values: %s', slot_values)

    # Retrieve "remembered" slot values from session attributes
    slot_values = get_remembered_slot_values(slot_values, session_attributes)
    logger.debug('<<Jasper>> "top_intent_handler(): slot_values afer get_remembered_slot_values: %s', slot_values)

    if slot_values.get('count') is None:
        slot_values['count'] = TOP_DEFAULT_COUNT

    if slot_values.get('dimension') is None:
        response_string = 'Please tell me a dimension, for example, "top five months".'
        return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': response_string})   

    # If switching dimension, forget the prior remembered value for that dimension
    dimension_slot = DIMENSIONS.get(slot_values.get('dimension')).get('slot')
    if dimension_slot is not None:
        slot_values[dimension_slot] = None
        logger.debug('<<Jasper>> "top_intent_handler(): cleared dimension slot: %s', dimension_slot)

    # store updated slot values
    logger.debug('<<Jasper>> "top_intent_handler(): calling remember_slot_values_NEW: %s', slot_values)
    remember_slot_values(slot_values, session_attributes)

    # Check for minimum required slot values
    # TODO test this
    if slot_values.get('dimension') is None:
        return close(session_attributes, 'Fulfilled',
            {'contentType': 'PlainText', 'content': "Sorry, I didn't understand that.  Try \"Top 5 venues for all rock and pop\"."})  

    """
    Example Query:
        SELECT e.event_name, sum(s.amount) total_sales
          FROM sales s, event e, venue v, category c, date_dim d
         WHERE e.event_id = s.event_id
           AND v.venue_id = e.venue_id
           AND c.cat_id = e.cat_id
           AND e.date_id = e.date_id
           AND v.venue_state = 'CA'
         GROUP BY e.event_name
         ORDER BY total_sales desc limit 5
    """

    # Build and execute query 
    # TODO: (REPLACE WITH FUNCTION TAKES SELECT CLAUSE AS STRING PARAMETER?)
    try:
        # the SELECT clause is for a particular dimension e.g., top 5 {states}...
        # Example: "SELECT {}, SUM(s.amount) ticket_sales FROM sales s, event e, venue v, category c, date_dim ed  "
        select_clause = TOP_SELECT.format(DIMENSIONS.get(slot_values.get('dimension')).get('column'))
    except KeyError:
        # TODO: is this necessary?
        return close(session_attributes, 'Fulfilled',
            {'contentType': 'PlainText', 'content': "Sorry, I don't know what you mean by " + slot_values['dimension']})
            
    # add JOIN clauses 
    # Example: " WHERE e.event_id = s.event_id AND v.venue_id = e.venue_id AND c.cat_id = e.cat_id AND d.date_id = e.date_id "
    where_clause = TOP_JOIN

    # add WHERE clause for each non empty slot
    # Example: " AND LOWER({}) LIKE LOWER('%{}%') " 
    for dimension in DIMENSIONS:
        slot_key = DIMENSIONS.get(dimension).get('slot')
        if slot_values[slot_key] is not None:
            value = pre_process_query_value(slot_key, slot_values[slot_key])
            where_clause += TOP_WHERE.format(DIMENSIONS.get(dimension).get('column'), value)

    try:
        # the GROUP BY is by dimension, and the ORDER by is the aggregated fact
        # Example: " GROUP BY {} ORDER BY ticket_sales desc"
        order_by_group_by = TOP_ORDERBY.format(DIMENSIONS.get(slot_values.get('dimension')).get('column'))
        order_by_group_by += " LIMIT {}".format(slot_values.get('count'))
    except KeyError:
        # TODO: is this necessary?
        return close(
            session_attributes,
            'Fulfilled',
            {
                'contentType': 'PlainText',
                'content': "Sorry, I don't know what you mean by " + dimension
            }
        )  

    query_string = select_clause + where_clause + order_by_group_by
    logger.debug('<<Jasper>> Athena Query String = ' + query_string)            

    # execute Athena query
    response = execute_athena_query(query_string)

    # Build response text for Lex
    response_string = ''
    result_count = len(response['ResultSet']['Rows']) - 1
    
    if result_count < int(slot_values.get('count', 0)):
        if result_count == 0:
            response_string += "There weren't any " + slot_values.get('dimension') + " "
        elif result_count == 1:
            response_string += "There was only 1. "
        else:
            response_string += "There were only " + str(result_count) + ". "

    if result_count == 0:
        pass
    elif result_count == 1:
        try:
            response_string += 'The top ' + DIMENSIONS.get(slot_values.get('dimension')).get('singular')
        except KeyError:
            response_string += 'The top ' + slot_values.get('dimension')
    else:
        response_string += 'The top ' + str(result_count) + ' ' + slot_values.get('dimension')
  
    # add the English versions of the WHERE clauses
    for dimension in DIMENSIONS:
        slot_key = DIMENSIONS[dimension].get('slot')
        logger.debug('<<Jasper>> pre top5_formatter[%s] = %s', slot_key, slot_values.get(slot_key))
        if slot_values.get(slot_key) is not None:
            # the DIMENSION_FORMATTERS perform a post-process functions and then format the output
            # Example:  {... 'venue_state': {'format': ' in the state of {}',  'function': get_state_name}, ...}
            if DIMENSION_FORMATTERS.get(slot_key) is not None:
                output_text = DIMENSION_FORMATTERS[slot_key]['function'](slot_values.get(slot_key))
                output_text = DIMENSION_FORMATTERS[slot_key]['format'].lower().format(output_text)
                response_string += ' ' + output_text
                logger.debug('<<Jasper>> top5_formatter[%s] = %s', slot_key, output_text)

    if result_count == 0:
        pass
    elif result_count == 1:
        response_string += ' was '
    else:
        response_string += ' were '
    
    # add the list of top X dimension values to the response text
    if result_count > 0:
        remembered_value = None    
        for counter, item in enumerate(response['ResultSet']['Rows']):
            if counter > 0:
                if counter > 1:
                    response_string += '; and ' if counter == result_count else '; '
                if result_count > 1:
                    response_string += str(counter) + ', '
                    
                value = post_process_dimension_output(slot_values.get('dimension'), item['Data'][0]['VarCharValue'])
                response_string += value
    
                remembered_value = item['Data'][0]['VarCharValue']

    response_string += '.'

    logger.debug('<<Jasper>> response_string = ' + response_string) 

    # If result count = 1, remember the value for future questions
    if result_count == 1:
        slot_name = DIMENSIONS.get(slot_values.get('dimension')).get('slot')
        slot_values[slot_name] = remembered_value

        # store updated query attributes
        remember_slot_values(slot_values, session_attributes)

    method_duration = time.perf_counter() - method_start
    method_duration_string = 'method time = %.0f' % (method_duration * 1000) + ' ms'
    logger.debug('<<Jasper>> "Method duration is: ' + method_duration_string) 
    
    logger.debug('<<Jasper>> top_intent_handler() - sessions_attributes = %s, response = %s', session_attributes, {'contentType': 'PlainText','content': response_string})

    return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': response_string})   


#
# parameters for Refresh intent
#
REFRESH_QUERY = 'SELECT DISTINCT event_name from event ORDER BY event_name'
REFRESH_SLOT = 'event_name'
REFRESH_INTENT = 'Compare_Intent'
REFRESH_BOT = 'Jasper'

def refresh_intent_handler(intent_request):
    athena = boto3.client('athena')

    # Build and execute query
    logger.debug('<<Jasper>> Athena Query String = ' + REFRESH_QUERY)            

    response = athena.start_query_execution(
        QueryString=REFRESH_QUERY,
        QueryExecutionContext={'Database': ATHENA_DB},
        ResultConfiguration={
            'OutputLocation': ATHENA_OUTPUT_LOCATION,
        }
    )

    query_execution_id = response['QueryExecutionId']

    start = time.perf_counter()
    status = 'RUNNING'
    while (status == 'RUNNING'):
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if (status == 'RUNNING'):
            #logger.debug('<<Jasper>> query status = ' + status + ': sleep 200ms') 
            time.sleep(0.200)

    duration = time.perf_counter() - start
    duration_string = 'query duration = %.0f' % (duration * 1000) + ' ms'
    logger.debug('<<Jasper>> query status = ' + status + ', ' + duration_string) 

    st_values = []
    response = athena.get_query_results(QueryExecutionId=query_execution_id, MaxResults=100)
    logger.debug('<<Jasper>> query response = ' + json.dumps(response)) 

    while len(response['ResultSet']['Rows']) > 0:
        for item in response['ResultSet']['Rows']:
            st_values.append({'value': item['Data'][0]['VarCharValue']})
            logger.debug('<<Jasper>> appending: ' + item['Data'][0]['VarCharValue']) 
        
        try:
            next_token = response['NextToken']
            response = athena.get_query_results(QueryExecutionId=query_execution_id, NextToken=next_token, MaxResults=100)
            logger.debug('<<Jasper>> additional query response = ' + json.dumps(response)) 
        except KeyError:
            break

    logger.debug('<<Jasper>> "st_values = ' + pprint.pformat(st_values)) 
        
    lex_models = boto3.client('lex-models')
    response = lex_models.get_slot_type(name=REFRESH_SLOT, version='$LATEST')
    logger.debug('<<Jasper>> "boto3 version = ' + boto3.__version__) 
    logger.debug('<<Jasper>> "Lex slot event_name = ' + pprint.pformat(response, indent=4)) 
    logger.debug('<<Jasper>> "Lex slot event_name checksum = ' + response['checksum']) 
    logger.debug('<<Jasper>> "Lex slot event_name valueSelectionStrategy = ' + response['valueSelectionStrategy']) 
    
    try:
        logger.debug('<<Jasper>> "st_values = ' + pprint.pformat(st_values)) 

        st_checksum = response['checksum']
        response = lex_models.put_slot_type(name=response['name'],
                                            description=response['description'],
                                            enumerationValues=st_values,
                                            checksum=response['checksum'],
                                            valueSelectionStrategy=response['valueSelectionStrategy']
                                            )
    except KeyError:
        pass
    
    response = lex_models.get_intent(name=REFRESH_INTENT, version='$LATEST')
    logger.debug('<<Jasper>> Lex get-intent = ' + pprint.pformat(response, indent=4)) 
    logger.debug('<<Jasper.. Lex get-intent keys = ' + pprint.pformat(response.keys()))
    
    response = lex_models.put_intent(name=response['name'],
                                     description=response['description'],
                                     slots=response['slots'],
                                     sampleUtterances=response['sampleUtterances'],
                                     conclusionStatement=response['conclusionStatement'],
                                     fulfillmentActivity=response['fulfillmentActivity'],
                                     checksum=response['checksum']
                                    )
    
    ## TODO: need to update all Intents that use the slot, to rev the version referenced.

    response = lex_models.get_bot(name=REFRESH_BOT, versionOrAlias='$LATEST')
    logger.debug('<<Jasper>> Lex bot = ' + pprint.pformat(response, indent=4)) 
    
    response = lex_models.put_bot(name=REFRESH_BOT,
                                  description=response['description'],
                                  intents=response['intents'],
                                  clarificationPrompt=response['clarificationPrompt'],
                                  abortStatement=response['abortStatement'],
                                  idleSessionTTLInSeconds=response['idleSessionTTLInSeconds'],
                                  voiceId=response['voiceId'],
                                  processBehavior='SAVE',
                                  locale=response['locale'],
                                  checksum=response['checksum'],
                                  childDirected=response['childDirected']
                                 )

    logger.debug('<<Jasper>> Lex put bot = ' + pprint.pformat(response, indent=4)) 

    # TODO: this seems to be building automatically based on that ^ parameter BUILD there.  Says: NOT_BUILT
    response_string = "I've refreshed the events dimension from the database.  Please rebuild me."
    return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText','content': response_string})   


def execute_athena_query(query_string):
    # TODO: change this to a decorator
    start = time.perf_counter()

    athena = boto3.client('athena')

    response = athena.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={'Database': ATHENA_DB},
        ResultConfiguration={
            'OutputLocation': ATHENA_OUTPUT_LOCATION,
        }
    )

    query_execution_id = response['QueryExecutionId']

    status = 'RUNNING'
    while (status == 'RUNNING'):
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if (status == 'RUNNING'):
            #logger.debug('<<Jasper>> query status = ' + status + ': sleep 200ms') 
            time.sleep(0.200)

    # TODO: clean up files in Athena output bucket

    duration = time.perf_counter() - start
    duration_string = 'query duration = %.0f' % (duration * 1000) + ' ms'
    logger.debug('<<Jasper>> query status = ' + status + ', ' + duration_string) 

    response = athena.get_query_results(QueryExecutionId=query_execution_id)
    logger.debug('<<Jasper>> query response = ' + json.dumps(response)) 

    return response


def get_slot_values(slot_values, intent_request):
    if slot_values is None:
        slot_values = {key: None for key in SLOT_CONFIG}
    
    slots = intent_request['currentIntent']['slots']

    for key,config in SLOT_CONFIG.items():
        slot_values[key] = slots.get(key)
        logger.debug('<<Jasper>> retrieving slot value for %s = %s', key, slot_values[key])
        if slot_values[key]:
            if config.get('type', ORIGINAL_VALUE) == TOP_RESOLUTION:
                # get the resolved slot name of what the user said/typed
                if len(intent_request['currentIntent']['slotDetails'][key]['resolutions']) > 0:
                    slot_values[key] = intent_request['currentIntent']['slotDetails'][key]['resolutions'][0]['value']
                else:
                    errorMsg = SLOT_CONFIG[key].get('error', 'Sorry, I don\'t understand "{}".')
                    raise SlotError(errorMsg.format(slots.get(key)))
                
            slot_values[key] = post_process_slot_value(key, slot_values[key])
    
    return slot_values


def get_remembered_slot_values(slot_values, session_attributes):
    str = session_attributes.get('rememberedSlots')
    remembered_slot_values = json.loads(str) if str is not None else {key: None for key in SLOT_CONFIG}
    
    if slot_values is None:
        slot_values = {key: None for key in SLOT_CONFIG}
    
    logger.debug('<<Jasper>> get_remembered_slot_values() - slot_values: %s', slot_values)
    logger.debug('<<Jasper>> get_remembered_slot_values() - remembered_slot_values: %s', remembered_slot_values)
    for key,config in SLOT_CONFIG.items():
        if config.get('remember', False):
            logger.debug('<<Jasper>> get_remembered_slot_values() - slot_values[%s] = %s', key, slot_values.get(key))
            logger.debug('<<Jasper>> get_remembered_slot_values() - remembered_slot_values[%s] = %s', key, remembered_slot_values.get(key))
            if slot_values.get(key) is None:
                slot_values[key] = remembered_slot_values.get(key)
                
    return slot_values


def remember_slot_values(slot_values, session_attributes):
    if slot_values is None:
        slot_values = {key: None for key,config in SLOT_CONFIG.items() if config['remember']}
    session_attributes['rememberedSlots'] = json.dumps(slot_values)
    logger.debug('<<Jasper>> Storing updated slot values: %s', slot_values)           
    return slot_values


def get_jasper_config():
    global ATHENA_DB
    global ATHENA_OUTPUT_LOCATION

    try:
        ATHENA_DB = os.environ['ATHENA_DB']
        ATHENA_OUTPUT_LOCATION = os.environ['ATHENA_OUTPUT_LOCATION']
# BTY DELETE        str = os.environ['dimensions']
    except KeyError:
        return 'I have a configuration error - please set up the Athena database information.'

    logger.debug('<<Jasper>> athena_db = ' + ATHENA_DB)
    logger.debug('<<Jasper>> athena_output_location = ' + ATHENA_OUTPUT_LOCATION)


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }
    
    logger.debug('<<Jasper>> "Lambda fulfillment function response = \n' + pprint.pformat(response, indent=4)) 

    return response


def increment_counter(session_attributes, counter):
    counter_value = session_attributes.get(counter, '0')

    if counter_value: count = int(counter_value) + 1
    else: count = 1
    
    session_attributes[counter] = count

    return count


# adjust dimension values as necessary prior to inserting into where clause
def pre_process_query_value(key, value):
    logger.debug('<<Jasper>> pre_process_query_value(%s, %s)', key, value)
    value = value.replace("'", "''")    # don't allow any 's in WHERE clause
    if key == 'event_month':
        value = value[0:3]
    elif key == 'venue_name':
        value = value.lower().replace('theater', 'theatre')
        value = value.lower().replace('u. s.', 'us')
        value = value.lower().replace('u.s.', 'us')
    elif key == 'venue_state':
        # TODO: CONFIRM IF THIS IS NEEDED HERE, SEE post_process below.  But this may be belt/suspenders
        value = US_STATES.get(value.lower(), value)

    logger.debug('<<Jasper>> pre_process_query_value() - returning key=%s, value=%s', key, value)
       
    return value


# adjust slot values as necessary after reading from intent slots
def post_process_slot_value(key, value):
    if key == 'venue_state':
        value = US_STATES.get(value.lower(), value)
        logger.debug('<<Jasper>> post_process_slot_value() - returning key=%s, value=%s', key, value)
    return value


def post_process_dimension_output(key, value):
    logger.debug('<<Jasper>> post_process_dimension_output(%s, %s)', key, value)
    if key == 'states':
        value = get_state_name(value)
    elif key == 'months':
        value = get_month_name(value)
    logger.debug('<<Jasper>> post_process_dimension_output() - returning key=%s, value=%s', key, value)
    return value


# helper functions for pre- and post-processors
def get_state_name(value):
    if not isinstance(value, str): return value
    state_name = REVERSE_US_STATES.get(value.upper())
    return state_name if state_name else value.title()


def get_month_name(value):
    if not isinstance(value, str): return value
    month_name = MONTH_NAMES.get(value.upper()[0:3])
    return month_name if month_name else value.title()


def post_process_venue_name(value):
    # TODO: get the value as it appears in the database
    if not isinstance(value, str): return value
    value = value.title().replace('Us ', 'US ')
    return value

INTENT_CONFIG = {
    'Hello_Intent':      {'handler': hello_intent_handler},
    'Count_Intent':      {'handler': count_intent_handler},
    'Compare_Intent':    {'handler': compare_intent_handler},
    'Top_Intent':        {'handler': top_intent_handler},
    'Reset_Intent':      {'handler': reset_intent_handler},
    'Switch_Intent':     {'handler': switch_intent_handler},
    'Refresh_Intent':    {'handler': refresh_intent_handler},
    'GoodBye_Intent':    {'handler': goodbye_intent_handler}
}

DIMENSION_FORMATTERS = {
    'event_name':  {'format': 'For {}',              'function': str.title},
    'event_month': {'format': 'In the month of {}',  'function': get_month_name},
    'venue_name':  {'format': 'At {}',               'function': post_process_venue_name},
    'venue_city':  {'format': 'In the city of {}',   'function': str.title},
    'venue_state': {'format': 'In the state of {}',  'function': get_state_name},
    'cat_desc':    {'format': 'For {}',              'function': str.title}
}

MONTH_NAMES = {
    "JAN": "January",
    "FEB": "February",
    "MAR": "March",
    "APR": "April",
    "MAY": "May",
    "JUN": "June",
    "JUL": "July",
    "AUG": "August",
    "SEP": "September",
    "OCT": "October",
    "NOV": "November",
    "DEC": "December"
}

US_STATES = {
    'alaska': 'AK',
    'alabama': 'AL',
    'arkansas': 'AR',
    'american samoa': 'AS',
    'arizona': 'AZ',
    'california': 'CA',
    'colorado': 'CO',
    'connecticut': 'CT',
    'district of columbia': 'DC',
    'delaware': 'DE',
    'florida': 'FL',
    'georgia': 'GA',
    'guam': 'GU',
    'hawaii': 'HI',
    'iowa': 'IA',
    'idaho': 'ID',
    'illinois': 'IL',
    'indiana': 'IN',
    'kansas': 'KS',
    'kentucky': 'KY',
    'louisiana': 'LA',
    'massachusetts': 'MA',
    'maryland': 'MD',
    'maine': 'ME',
    'michigan': 'MI',
    'minnesota': 'MN',
    'missouri': 'MO',
    'mississippi': 'MS',
    'montana': 'MT',
    'north carolina': 'NC',
    'north dakota': 'ND',
    'nebraska': 'NE',
    'new hampshire': 'NH',
    'new jersey': 'NJ',
    'new mexico': 'NM',
    'nevada': 'NV',
    'new york': 'NY',
    'ohio': 'OH',
    'oklahoma': 'OK',
    'oregon': 'OR',
    'pennsylvania': 'PA',
    'puerto rico': 'PR',
    'rhode island': 'RI',
    'south carolina': 'SC',
    'south dakota': 'SD',
    'tennessee': 'TN',
    'texas': 'TX',
    'utah': 'UT',
    'virginia': 'VA',
    'virgin islands': 'VI',
    'vermont': 'VT',
    'washington': 'WA',
    'wisconsin': 'WI',
    'west virginia': 'WV',
    'wyoming': 'WY'
}

REVERSE_US_STATES = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AS': 'American Samoa',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DC': 'District of Columbia',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'GU': 'Guam',
    'HI': 'Hawaii',
    'IA': 'Iowa',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'MA': 'Massachusetts',
    'MD': 'Maryland',
    'ME': 'Maine',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MO': 'Missouri',
    'MS': 'Mississippi',
    'MT': 'Montana',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'NE': 'Nebraska',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NV': 'Nevada',
    'NY': 'New York',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'PR': 'Puerto Rico',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VI': 'Virgin Islands',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming'
}

