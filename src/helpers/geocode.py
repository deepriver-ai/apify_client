import logging
import time
import requests
import json
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

GEOCODE_MAX_RETRIES = 3
GEOCODE_RETRY_SLEEP = 5  # seconds between retries

NLP_URL = os.getenv('NLP_URL')
GEOCODING_URL = os.getenv('GEOCODING_URL')

'''
ssh -J connect@home.deepriver.ai:4222 oscar@192.168.1.105 -L 8210:192.168.1.68:8210 -L 8200:192.168.1.68:8200
'''

# Load environment variables


levels = {'LUG': 7, 'CALLE': 6, 'COL': 5, 'MUN': 3, 'EST': 2, 'PAIS': 1}

default_dct = {'confidence': .6,
               'position_in_text': 1}

def format_mentions(main, context=None):
    if not type(main) is dict:
        main_entities = requests.post(NLP_URL,
                         json={'text': main, 'entities': 1},
                         headers={'Content-Type': 'application/json'},
                         timeout=10)
        main_entities = json.loads(main_entities.text)['entities']
    else:
        main_entities = main

    mentions = []
    for k, l in enumerate(levels.keys()):
        for mention, index in main_entities[l]:
            mention_dct = default_dct.copy()
            mention_dct['level'] = levels[l]
            mention_dct['text'] = mention
            mention_dct['position_in_text'] = index
            mention_dct['mention_id'] = len(mentions)
            mention_dct['context_group'] = 1
            mentions.append(mention_dct)

    if context:
        context_entities = requests.post(NLP_URL,
                         json={'text': context, 'entities': 1},
                         headers={'Content-Type': 'application/json'},
                         timeout=10)
        context_entities = json.loads(context_entities.text)['entities']

        for k, l in enumerate(levels.keys()):
            for mention, index in context_entities[l]:
                mention_dct = default_dct.copy()
                mention_dct['level'] = levels[l]
                mention_dct['text'] = mention
                mention_dct['position_in_text'] = index
                mention_dct['context_group'] = 2
                mention_dct['mention_id'] = len(mentions)
                mentions.append(mention_dct)

    return mentions


def geocode(text, context=None):
    '''
    dictionary of locations for each context group (text and context): each contains a list of all locations mentioned in the text.
    Returns {"error": <message>} if the geocoding service fails after max retries.
    '''
    for attempt in range(1, GEOCODE_MAX_RETRIES + 1):
        try:
            arguments = {'mentions': format_mentions(text, context)}
            response = requests.post(GEOCODING_URL,
                                     json=arguments,
                                     headers={'Content-Type': 'application/json'},
                                     timeout=10)
            response.raise_for_status()
            return json.loads(response.text)
        except Exception as ex:
            logger.warning("Geocoding attempt %d/%d failed: %s", attempt, GEOCODE_MAX_RETRIES, ex)
            if attempt < GEOCODE_MAX_RETRIES:
                time.sleep(GEOCODE_RETRY_SLEEP)

    logger.warning("Geocoding service unavailable after %d attempts, skipping location enrichment", GEOCODE_MAX_RETRIES)
    return {"error": f"Geocoding failed after {GEOCODE_MAX_RETRIES} attempts"}

'''
geocode sample response:

'1' is the first context group: text
'2' is the second context group: context

{'1': [{'level_1_id': '_840',
   'level_4_id': '',
   'precision_level': '1',
   'formatted_name': 'United States',
   'level_3_id': '',
   'level_1': 'united states',
   'level_2_id': '',
   'coords': {'lat': 30.88296, 'lon': -87.77305},
   'level_7_id': '',
   'geoid': '_840',
   'level_4': '',
   'level_5': '',
   'level_6': '',
   'level_7': '',
   'level_5_id': '',
   'level_6_id': '',
   'level_2': '',
   'level_3': ''},
  {'level_1_id': '_804',
   'level_4_id': '',
   'precision_level': '1',
   'formatted_name': 'Ukraine',
   'level_3_id': '',
   'level_1': 'ukrajina',
   'level_2_id': '',
   'coords': {'lat': 50.58518, 'lon': 34.4849},
   'level_7_id': '',
   'geoid': '_804',
   'level_4': '',
   'level_5': '',
   'level_6': '',
   'level_7': '',
   'level_5_id': '',
   'level_6_id': '',
   'level_2': '',
   'level_3': ''}],
 '2': [{'level_1_id': '_152',
   'level_4_id': '',
   'precision_level': '3',
   'formatted_name': 'Punta Arenas Provincia de Magallanes Magallanes Chile',
   'level_3_id': '_1520a001',
   'level_1': 'chile',
   'level_2_id': '_1520a',
   'coords': {'lat': -53.15483, 'lon': -70.91129},
   'level_7_id': '',
   'geoid': '_1520a001',
   'level_4': '',
   'level_5': '',
   'level_6': '',
   'level_7': '',
   'level_5_id': '',
   'level_6_id': '',
   'level_2': 'magallanes',
   'level_3': 'punta arenas'}]}

'''
