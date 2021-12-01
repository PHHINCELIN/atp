# Imports
import dataiku
from bs4 import element
import csv
import datetime
import os
import errno
import pandas.tseries.offsets as offsets
import encodings
import time
from tool_functions import *

# Code for custom code recipe get-rankings-history (imported from a Python recipe)

# To finish creating your custom recipe from your original PySpark recipe, you need to:
#  - Declare the input and output roles in recipe.json
#  - Replace the dataset names by roles access in your code
#  - Declare, if any, the params of your custom recipe in recipe.json
#  - Replace the hardcoded params values by acccess to the configuration map

# See sample code below for how to do that.
# The code of your original recipe is included afterwards for convenience.
# Please also see the "recipe.json" file for more information.

# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *

# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role.
# Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.

# PH : No input for this scrapping Recipe
# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
#input_A_names = get_input_names_for_role('input_A_role')
# The dataset objects themselves can then be created like this:
#input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

# For outputs, the process is the same:
# Get a handle for the output dataset
output_datasets = get_output_names_for_role('main_output')
output_dataset = [dataiku.Dataset(name) for name in output_datasets]

# The configuration consists of the parameters set up by the user in the recipe Settings tab.

# Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# user will be prompted for values.

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
#my_variable = get_recipe_config()['parameter_name']

# For optional parameters, you should provide a default value in case the parameter is not present:

# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])


#############################
# Your original recipe
#############################

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
"""Scrap Tennis matches results on ATP.

- One shot scrapping to get historic results
- source : http://www.atpworldtour.com/en/scores/results-archive
"""

# Constants
ATP_BSE_URL = 'http://www.atpworldtour.com'
ATP_MATCHES_RESULTS_URL = '/en/scores/results-archive'


def get_tourneys(page_tourneys):
    """Extract information about ongoing tourneys"""
    # Get page content
    soup = get_soup(page_tourneys)

    tourneys_soup = soup.find('table', attrs={'class': 'results-archive-table mega-table'}).\
        find_all('tr', attrs={'class': 'tourney-result'})

    for r in tourneys_soup:
        tourney = {}
        
        if r.find('div', class_='tourney-detail-winner') is None:
            print("print 1")
            print(r)
            # Only handle ongoing tourneys, when winner is still unkown
            tourney_date_start = r.find('span', class_='tourney-dates').string.strip() #YYYY.mm.dd
            tourney_date_start = tourney_date_start.replace('.', '-')
            tourney_date_start = datetime.datetime.strptime(tourney_date_start, '%Y-%m-%d')
            tourney['start_date'] = datetime.datetime.strftime(tourney_date_start, '%Y-%m-%d')

            if tourney_date_start < datetime.datetime.now():
                # Only handle tourneys that have started
				print("print 2")
				print(r)
				try:
					tourney['name'] = r.find('a', class_='tourney-title').string.strip()
					tourney['location'] = r.find('span', class_='tourney-location').string.strip()
					tourney['draw_single'] = r.find('td', class_='tourney-details').div.div.span.string.strip()
					tourney['indoor_outdoor'] = r.find('td', class_='tourney-details').findNext('td').div.div.contents[0].strip()
					tourney['surface'] = r.find('td', class_='tourney-details').findNext('td').div.div.span.string.strip()
					tourney['prize_money'] = r.find('td', class_='tourney-details fin-commit').div.div.span.string.strip()
					tourney['daily_schedule_url'] = r.find('a', class_='button-border').get('href').replace('live-scores', 'daily-schedule')
					print("c'est ICI")
					print(tourney)
					yield tourney
				except Exception as e:
					print('ERROR: missing information for tourney {0}'.format(tourney['name'].encode('utf8')))
            else:
                break
        else:
            continue

def get_matches_to_play(tourney):
    """Extract information about matches to play, where we can forecast the output"""
    soup = get_soup('http://www.atpworldtour.com' + tourney['daily_schedule_url'])
    #daily_matches = soup.find('div', class_= 'sectioned-day-tables').table.tbody.find_all('tr')
    
    try:
        tourney_matches = soup.find('div', class_= 'sectioned-day-tables').find_all('tr')
        try:
            for m in tourney_matches:
                print("C'est LAAAA")

                match = {}
                print(m)
                if m.find(class_='day-table-heading'):
                    # discard header line
                    continue
                if m.find(class_='day-table-section-title'):
                    # discard title line
                    continue
                elif m.find('span', class_='tour-association-wta'):
                    # If tourney also includes WTA matches, exlude them
                    continue
               # elif len(m.find('td', class_='day-table-button').contents)<2:
                    # If tourney includes doubles, exclude them. (Only ATP singles matches have the H2H button)
                    #continue
                else:
                    try: 
                        #print(m.find('div', class_='day-table-player-group').contents)
                        match['round'] = m.find('td', class_='day-table-round').string.strip()#.decode('utf8')
                        print('ROUND')
                        print(match['round'])
                        match['player1'] = m.find('td', class_='day-table-name').a.string
                        match['player2'] = m.find('div', class_='day-table-player-group').a.string
                        match.update(tourney)
                        print(match)
                        #if len(m.find('div', class_='day-table-player-group').contents) == 3:

                    except Exception as e:
                        print e
                    else:
                        if m.find('td', class_='day-table-button').a:
                            # We only keep single. Doubles are filtered with the fact there is no H2H stats
                            yield match
        except Exception as e:
            print(e)
            print('Error : Unable to check daily schedule for {0}'.format(tourney['name'].encode('utf8')))
            match['error'] = u'Error while trying to access daily schedule for tourney {0} at {1}'.format(tourney['name'], datetime.datetime.now().strftime('%Y-%m-%d:%H-%M-%S'))
            yield match
    except Exception as e:
        print(e)
        print("It does not seem to have valid ongoing matches here")
        yield {}
    
    
        
        
def build_matches_dataset():
    """
    """

    gen_tourneys = get_tourneys(ATP_BSE_URL+ATP_MATCHES_RESULTS_URL)
    print('Getting matches at {0}'.format(datetime.datetime.now().strftime('%Y-%m-%d:%H-%M-%S')))
    for tourney in gen_tourneys:
        start_time = time.time()
        gen_matches = get_matches_to_play(tourney)
        for match in gen_matches:
            yield match
        duration = time.time() - start_time
        print('It took {0}secondes to get all matches from {1}'.format(duration, tourney['name'].encode('utf8')))
        
        # BELOW : to update


# Recipe outputs
atp_matches_to_play_dataset = output_dataset[0]
atp_matches_to_play_dataset.write_schema([
{"name": "name",
"type": "string"
},
{"name": "start_date",
"type": "string"
},
{"name": "location",
"type": "string"
},
{"name": "draw_single",
"type": "string"
},
{"name": "prize_money",
"type": "string"
},    
{"name": "indoor_outdoor",
"type": "string"
},
{"name": "surface",
"type": "string"
},
{"name": "round",
"type": "string"
},
{"name": "player1",
"type": "string"
},
{"name": "player2",
"type": "string"
},
{"name": "error",
"type": "string"
}
])

my_gen = build_matches_dataset()

with atp_matches_to_play_dataset.get_writer() as writer:
    for match_to_play in my_gen:
        print('MATCH WRITEN                -----------')
        print(match_to_play)
        if match_to_play:
            writer.write_row_dict(match_to_play)
