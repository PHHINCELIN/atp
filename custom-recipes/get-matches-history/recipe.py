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
output_dataset_partioned = output_dataset[0].get_config()['partitioning']['dimensions']

# The configuration consists of the parameters set up by the user in the recipe Settings tab.

# Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# user will be prompted for values.

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
#my_variable = get_recipe_config()['parameter_name']

# For optional parameters, you should provide a default value in case the parameter is not present:
use_partition = get_recipe_config().get('usePartition')
if use_partition :
    date_start = datetime.datetime.strptime(dataiku.dku_flow_variables["DKU_DST_partition_month"], '%Y-%m')
    date_stop = date_start + offsets.MonthEnd()
else:
    # Check if partionioning is activated
    if output_dataset_partioned:
        raise ValueError('Output dataset is partioned, please tick partition boxe')
    else:
        # Get parameters 'YYYY-mm-dd', string format
        date_start = get_recipe_config().get('date_start')
        date_stop = get_recipe_config().get('date_stop')
        # Validate formating and parse as dates object
        date_start = validate_date(date_start)
        date_stop = validate_date(date_stop)
detailled_stats = get_recipe_config().get('detailled_stats')

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
ATP_BSE_URL = 'https://www.atptour.com/'
ATP_MATCHES_RESULTS_URL = '/en/scores/results-archive'


def get_years(page_results, date_start, date_stop):
    """Extract urls containing rankings history (One url per week)."""
    # Extract years from dates
    year_start = date_start.year
    year_stop = date_stop.year

    # Get page content
    soup = get_soup(page_results)
    print("THIS DA SOUPE")
    print(soup)

    # Locate the weeks in a scroll button
    years = soup.find(name='ul', attrs={'data-value': 'year'}).\
        find_all(name='li', attrs={'data-value': True, 'style': False})
    for w in years:
        year = {}
        year['year'] = w.get('data-value')
        year['results_url'] = page_results + "?year=" + \
            str(w.get('data-value'))
        if int(year['year']) < year_start:
            continue  # Matches results are only scrapped for requested years
        elif int(year['year']) > year_stop:
            continue  # Matches results are only scrapped for requested years
        else:
            yield year

def get_tourneys(page_tourneys, date_start, date_stop):
    """Extract information about tourneys for a given year."""
    # Get page content
    soup = get_soup(page_tourneys)

    tourneys_soup = soup.find('table', attrs={'class': 'results-archive-table mega-table'}).\
        find_all('tr', attrs={'class': 'tourney-result'})

    for r in tourneys_soup:
        tourney = {}
        tourney_date_start = r.find('span', class_='tourney-dates').string.strip() #YYYY.mm.dd
        tourney_date_start = tourney_date_start.replace('.', '-')
        tourney_date_start = datetime.datetime.strptime(tourney_date_start, '%Y-%m-%d')
        today = datetime.datetime.now()
        date_check = tourney_date_start.strftime("%Y-%m-%d")
        date_start_check = date_start.strftime("%Y-%m-%d")
        print(date_check, date_start_check)
        
        # First if is a special case for year 2019 where first tourneys have a date_start on the 2018
        if date_start_check == '2019-01-01' and date_check == '2018-12-31':
            try:
                tourney['start_date'] = datetime.datetime.strftime(tourney_date_start, '%Y-%m-%d')
                tourney['name'] = r.find('a', class_='tourney-title').string.strip()
                tourney['location'] = r.find('span', class_='tourney-location').string.strip()
                tourney['draw_single'] = r.find('td', class_='tourney-details').div.div.span.string.strip()
                tourney['indoor_outdoor'] = r.find('td', class_='tourney-details').findNext('td').div.div.contents[0].strip()
                tourney['surface'] = r.find('td', class_='tourney-details').findNext('td').div.div.span.string.strip()
                tourney['prize_money'] = r.find('td', class_='tourney-details fin-commit').div.div.span.string.strip()
                tourney['results_url'] = r.find('a', class_='button-border').get('href')
                if 'current' in tourney['results_url']:
                    # Ongoing tourney, special URL http://www.atpworldtour.com/en/scores/current/tourney_name/tourney_ID/live-scores
                    tourney['results_url'] = tourney['results_url'].replace('live-scores', 'results')
                yield tourney
            except Exception as e:
                print('Error : No information for this tourney {0} '.format(tourney['name'].encode('utf8')), e)

        elif tourney_date_start > today :
            continue # Only gets matches history
        elif tourney_date_start < date_start :
            continue
        elif tourney_date_start > date_stop :
            continue
        else:
            try:
                tourney['start_date'] = datetime.datetime.strftime(tourney_date_start, '%Y-%m-%d')
                tourney['name'] = r.find('a', class_='tourney-title').string.strip()
                tourney['location'] = r.find('span', class_='tourney-location').string.strip()
                tourney['draw_single'] = r.find('td', class_='tourney-details').div.div.span.string.strip()
                tourney['indoor_outdoor'] = r.find('td', class_='tourney-details').findNext('td').div.div.contents[0].strip()
                tourney['surface'] = r.find('td', class_='tourney-details').findNext('td').div.div.span.string.strip()
                tourney['prize_money'] = r.find('td', class_='tourney-details fin-commit').div.div.span.string.strip()
                tourney['results_url'] = r.find('a', class_='button-border').get('href')
                if 'current' in tourney['results_url']:
                    # Ongoing tourney, special URL http://www.atpworldtour.com/en/scores/current/tourney_name/tourney_ID/live-scores
                    tourney['results_url'] = tourney['results_url'].replace('live-scores', 'results')
                print(tourney)
                yield tourney
            except Exception as e:
                print(tourney, e)
                #print('Error : No information for this tourney {0} '.format(tourney['name'].encode('utf8')), e)


def get_tourney_results(page_tourney, tourney):
    """Extract the results of a given tourney.

    - winner
    - loser
    - score (64 64 means 2 sets victory / 754 63 means 2 sets victory with a tie-break in the 1st set won 7 points to 4
    - url with detailed stats of the match for further development
    """
    try:
        soup = get_soup(page_tourney)

        for c in soup.find('table', class_='day-table').children:
            if isinstance(c, element.NavigableString):
                continue  # go to the next iteration if it is a blank line
            if isinstance(c, element.Tag):
                if c.name == 'thead':
                    round_name = c.tr.th.string
                if c.name == 'tbody':
                    matches_soup = c.find_all('tr')
                    for m in matches_soup: 
                        try:        
                            match = {}
                            stats = {}
                            result= {}
                            match['round'] = round_name
                            match['winner'] = ' '.join(m.find('td', class_='day-table-name').a.string.split())
                            match['loser'] = ' '.join(m.find('td', class_='day-table-name').findNext('td', class_='day-table-name').a.string.split())
                            match['score'] = m.find('td', class_='day-table-score').a.text.strip()
                            if m.find('td', class_='day-table-score').a.get('href') is not None:  # No URL for Walkover
                                match['stats_url'] = ATP_BSE_URL + m.find('td', class_='day-table-score').a.get('href')  # could be used to get detailed stats
                                stats = get_match_stats(match['stats_url'])
                                sleeper(1,1)
                            else:
                                match['stats_url'] = ''
                            print('getting match result {0} vs {1} at {2} - {3}'.format(match['winner'].encode('utf8'), match['loser'].encode('utf8'), tourney['name'].encode('utf8'), tourney['start_date'].encode('utf8')))
                        except Exception as e:
                            print('Error ', e)
                            result['error'] =u'no information for match {0} vs {1} at {2}'.format(match['winner'].encode('utf8'), match['loser'].encode('utf8'), tourney['name'].encode('utf8'))
                        finally:
                            result.update(match)
                            result.update(tourney)
                            yield result
    except Exception as e:
        print('Error : No information for this tourney {0} '.format(tourney['name'].encode('utf8')), e)
        result = {}
        result.update(tourney)
        result['error'] = u'No information for tourney {0} '.format(tourney['name'])
        yield result


def  get_match_stats(match):
    """Extract all the stats from a given match."""
    start_time = time.time()
    #driver = create_driver()
    try:
        #soup = get_soup2(match['stats_url'], driver)
        sleeper(5,10)
        soup = get_soup(match['stats_url'])
        print("printin la soupe de stat")
        print(soup)
        match_stats = {}
        #print('getting getting detailled stats for match {0} vs {1} at {2} - {3}'.format(match['winner'].encode('utf8'), match['loser'].encode('utf8'), match['name'].encode('utf8'), match['start_date'].encode('utf8')))
        #print("WE ARE HERE")
        match_stat_soup_table = soup.find('table', attrs={'class': 'match-stats-table'})
        #print("printing the table")
        #print(match_stat_soup_table)
        match_stat_soup = match_stat_soup_table.find_all('tr', attrs={'class': 'match-stats-row percent-on'})
        #print("la Soupe de MAtches ______")
        #print(match_stat_soup)
        for c  in match_stat_soup:
            print("printing stat line _____")
            print(c)
            winner_stat = c.find('td', attrs={'class': 'match-stats-number-left'}).text.strip().strip("%\n ")
            #print("winner_stat :", winner_stat)
            loser_stat = c.find('td', attrs={'class': 'match-stats-number-right'}).text.lower().strip()
            stat_name = c.find('td', attrs={'class': 'match-stats-label'}).text.strip().lower().strip().replace(' ', '_')         
            match_stats['winner_' + stat_name] = winner_stat
            match_stats['loser_' + stat_name] = loser_stat
            print("print stats _______")
            print(match_stats)
        match.update(match_stats)
        #print("printing match ___________")
        #print(match)
        duration = time.time() - start_time
        print('It took {0}secondes to get stats for match {1} vs {2}'.format(duration, match['winner'].encode('utf8'), match['loser'].encode('utf8')))

        yield match
        
        #delete_driver(driver)
        
    except Exception as e:
        print('No detailled stats here')
        print(e)
        yield match
        
        #delete_driver(driver)
    
def build_matches_results_history(date_start, date_stop, detailled_stats):
    """For each year available on http://www.atpworldtour.com/en/scores/results-archive :
    build a .csv that contains all matches result (tourney description, winner, loser, score)

    - date_start and date_stop : control history depth. ATTENTION date_start < date_stop
    - nb_years : control history depth
    """
    # Number of Get Request on www.atpworldtour.com = nb_years * nb_yearly_touneys * nb_matches_per_tourney + 1 (to get years list)
    for year in get_years(ATP_BSE_URL+ATP_MATCHES_RESULTS_URL, date_start, date_stop):
        print('Getting {0} matches'.format(year))
        for tourney in get_tourneys(year['results_url'], date_start, date_stop):  # For a given year, go through all tourneys
            start_time = time.time()
            gen_matches = get_tourney_results(ATP_BSE_URL + tourney['results_url'], tourney)
            for match in gen_matches:
                if detailled_stats:
                    gen_matches_with_stats = get_match_stats(match)
                    for match_with_stats in gen_matches_with_stats:
                        yield match_with_stats
                else:
                    yield match
            duration = time.time() - start_time
            print('It took {0}secondes to get all matches from {1}'.format(duration, tourney['name'].encode('utf8')))
            


# Recipe outputs
atp_matches_history = output_dataset[0]
atp_matches_history.write_schema([
{"name": "name",
"type": "string"
},
{"name": "winner",
"type": "string"
},
{"name": "surface",
"type": "string"
},
{"name": "round",
"type": "string"
},
{"name": "loser",
"type": "string"
},
{"name": "indoor_outdoor",
"type": "string"
},
{"name": "score",
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
{"name": "stats_url",
"type": "string"
},
{"name": "results_url",
"type": "string"
},
{"name": "start_date",
"type": "string"
},
{"name": "loser_1st_serve",
"type": "string"
},
{"name": "loser_1st_serve_points_won",
"type": "string"
},
{"name": "loser_1st_serve_return_points_won",
"type": "string"
},
{"name": "loser_2nd_serve_points_won",
"type": "string"
},
{"name": "loser_2nd_serve_return_points_won",
"type": "string"
},
{"name": "loser_aces",
"type": "string"
},
{"name": "loser_break_points_converted",
"type": "string"
},
{"name": "loser_break_points_saved",
"type": "string"
},
{"name": "loser_double_faults",
"type": "string"
},
{"name": "loser_return_games_played",
"type": "string"
},
{"name": "loser_return_points_won",
"type": "string"
},
{"name": "loser_return_rating",
"type": "string"
},
{"name": "loser_serve_rating",
"type": "string"
},
{"name": "loser_service_games_played",
"type": "string"
},
{"name": "loser_service_points_won",
"type": "string"
},
{"name": "winner_1st_serve",
"type": "string"
},
{"name": "winner_1st_serve_points_won",
"type": "string"
},
{"name": "winner_1st_serve_return_points_won",
"type": "string"
},
{"name": "winner_2nd_serve_points_won",
"type": "string"
},
{"name": "winner_2nd_serve_return_points_won",
"type": "string"
},
{"name": "winner_aces",
"type": "string"
},
{"name": "winner_break_points_converted",
"type": "string"
},
{"name": "winner_break_points_saved",
"type": "string"
},
{"name": "winner_double_faults",
"type": "string"
},
{"name": "winner_return_games_played",
"type": "string"
},
{"name": "winner_return_points_won",
"type": "string"
},
{"name": "winner_return_rating",
"type": "string"
},
{"name": "winner_serve_rating",
"type": "string"
},
{"name": "winner_service_games_played",
"type": "string"
},
{"name": "winner_service_points_won",
"type": "string"
},
{"name": "error",
"type": "string"
}
])

my_gen = build_matches_results_history(date_start=date_start, date_stop=date_stop, detailled_stats=detailled_stats)

with atp_matches_history.get_writer() as writer:
    for match_results in my_gen:
        writer.write_row_dict(match_results)
