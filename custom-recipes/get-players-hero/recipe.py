import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from tool_functions import *
import datetime
import pandas.tseries.offsets as offsets
import urllib
from os import path, listdir
import time
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
# Get a handle for the output managed folder
output_folders = get_output_names_for_role('main_output')
output_folder = [dataiku.Folder(name) for name in output_folders]
# The configuration consists of the parameters set up by the user in the recipe Settings tab.

# Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# user will be prompted for values.

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
#my_variable = get_recipe_config()['parameter_name']

# For optional parameters, you should provide a default value in case the parameter is not present:

# Get parameters 'YYYY-mm-dd', string format
date_start = get_recipe_config().get('date_start')
date_stop = get_recipe_config().get('date_stop')
# Validate formating and parse as dates object
date_start = validate_date(date_start)
date_stop = validate_date(date_stop)

ranking_depth = int(get_recipe_config().get('ranking_depth'))
# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])


#############################
# Your original recipe
#############################

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#-*- coding: utf-8 -*-
"""Scrap Tennis players weekly ranks on ATP.

- One shot scrapping to get historic rankings
- Tool for weekly scraping when ATP plublish ranking update
- source : http://www.atpworldtour.com/en/rankings/singles
"""

ATP = "http://www.atpworldtour.com"


def get_page_weekly_rankings(page_rankings, date_start, date_stop, depth):
    """Extract urls containing rankings history (One url per week)

    depth controls how many players are wanted"""

    # Get page content
    soup = get_soup(page_rankings)

    # Locate the weeks in a scroll button
    weeks = soup.find(name ='ul', attrs={'data-value':'rankDate'}).\
    find_all(name='li', attrs={'data-value': True})

    for w in weeks:
        week = {}
        week['week'] = w.get('data-value')
        week['week_url'] = page_rankings + "?rankDate=" + \
        str(w.get('data-value')) + "&rankRange=0-" + str(depth)
        date_week = datetime.datetime.strptime(week['week'], '%Y-%m-%d')
        if date_week < date_start:
            continue
        elif date_week > date_stop:
            continue
        else:
            yield week

        
def get_player_hero_image(page, player_name):
    """Extract the url of player image.

    Check if the player-hero-image (.png) exists in the manage folder.
    If it does not exist download it with urllib/retrieve
    """
    # Get a handle on the Managed Folder where images are stored
    path_managed_folder = output_folder[0].get_path()
    # Check if player-hero-image already downloaded
    list_existing_images = listdir(path_managed_folder)
    list_existing_images_clear = [i.split('.')[0].replace('-', ' ') for i in list_existing_images]

    if player_name not in list_existing_images_clear:
        # Get page content
        soup = get_soup(page)

        # Get player hero-image
        if soup.find('div', class_='player-profile-hero-image'):
            hero_image_url = ATP + soup.find('div', class_='player-profile-hero-image').img.get('src')
            urllib.urlretrieve(hero_image_url, path.join(path_managed_folder, player_name.replace(' ', '-') + '.png'))
        else :
            print('Hero image is unavailable for {0}. Assigning standard image'.format(player_name.encode('utf8')))
            #with open(path.join(path_managed_folder,'standard-player-hero.png'), 'rb') as f:
                #data = f.read()
            with open(path.join(path_managed_folder, player_name.replace(' ', '-') + '.png'), 'wb') as f:
                f.write(u'No player hero image available')
    else:
        print('Player {0} already has a hero image'.format(player_name.encode('utf8')))


def get_players_info(page, week):
    """Extract players main info (name, ranking, age, points)."""
    # Get page content
    soup = get_soup(page)
    # locate players information
    players_soup = soup.tbody.find_all('tr')
    for p in players_soup:
        player = {}
        try:
            player['name'] = p.find('td', class_='player-cell').a.string
            player['page'] = ATP + p.find('td', class_='player-cell').a.get('href')
            yield player
        except Exception as e:
            print(e)
            continue


def scrap_players_heros(date_start, date_stop, ranking_depth):
    """For each ranking week available on atpworldtour.com :
    build a .csv with player (name, ranking, age, ATP points)

    - nb_weeks : controls number of weeks wanted in history (from most recent),
    default is 12 (about 3 months)
    - ranking_depth : controls max ranking of players, default is 500
    - Get all rankings between week_start and week_stop
    """
    ATP_RANKINGS = "http://www.atpworldtour.com/en/rankings/singles"
    for w in get_page_weekly_rankings(ATP_RANKINGS, date_start, date_stop, ranking_depth):
        start_time = time.time()
        print('Getting players heros for week {0}'.format(w['week']))
        gen_players_weekely_rankings = get_players_info(w['week_url'], w['week'])
        for player in gen_players_weekely_rankings:
            get_player_hero_image(player['page'], player['name'])
        duration = time.time() - start_time
        print('It took {0} secondes to get players heros for week {1}'.format(duration, w['week'] ))
       

scrap_players_heros(date_start=date_start, date_stop=date_stop, ranking_depth=ranking_depth)
