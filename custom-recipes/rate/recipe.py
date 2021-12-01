# Imports
import dataiku
from bs4 import element, BeautifulSoup
import csv
import datetime
import os
import errno
import pandas.tseries.offsets as offsets
import encodings
import time
import urllib2

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


def get_soup(page):
    """Get soup from an HTML page."""
    html_page = urllib2.urlopen(page).read()
    soup = BeautifulSoup(html_page, 'lxml')
    print('BOOM MAKING THE SOUP for {0}'.format(page))
    return soup


# Constants
COTE_BASE_URL1 = 'http://cotes.fr/tennis'
COTE_BASE_URL2 = 'http://cotes.fr/'


def get_tourneys(url):
    soup = get_soup(url)
    l = soup.find('div' , attrs={'id':'odds'}).find_all('li')
    for i in l:
        text = i.a.text
        url = i.a.get('href')
        #if 'Hommes' in text:
            #if 'Doubles' not in text:
        yield url
    

def get_rates(tourney_url):
    
    
    soup = get_soup(tourney_url)
    tourney_name = soup.find('div', attrs={'id':'oddetail'}).h1.text.strip()
    cotes = soup.find('table', attrs={'class':'bettable'}).find_all('tr')
    i = 0
    match = {}
    for c in cotes:
        if c.find('h2', class_='matchname'):
            if i > 0:
                yield match
            match = {}
            match['time_scheduled'] = c.find('h2', class_='matchname').next_sibling.string.strip()
            match['player1'] = c.find('h2', class_='matchname').a.string
            match['player2'] = c.find('h2', class_='matchname').a.next_sibling.next_sibling.string
            match['raw_text'] = c.text
            match['tourney_name'] = tourney_name
            match['rates'] = []
            i+=1
        if c.find('td', class_='bet'):
            booky_name = c.attrs['title'].split()[2]
            rate = (booky_name,(c.find('td', class_='bet').string.strip(),c.find('td', class_='bet').next_sibling.next_sibling.string.strip()))
            match['rates'].append(rate)
            

def build_rates_dataset():
    """
    """

    gen_tourneys = get_tourneys(COTE_BASE_URL1)
    print('Getting matches at {0}'.format(datetime.datetime.now().strftime('%Y-%m-%d:%H-%M-%S')))
    for tourney in gen_tourneys:
        start_time = time.time()
        gen_rates = get_rates(COTE_BASE_URL2 + tourney)
        for match in gen_rates:
            yield match
        duration = time.time() - start_time
        print('It took {0}secondes to get all matches from {1}'.format(duration, tourney.encode('utf8')))


# Recipe outputs
atp_rates_dataset = output_dataset[0]
atp_rates_dataset.write_schema([
{"name": "tourney_name",
"type": "string"
},
{"name": "player1",
"type": "string"
},
{"name": "player2",
"type": "string"
},
{"name": "raw_text",
"type": "string"
},
{"name": "rates",
"type": "string"
}
])

my_gen = build_rates_dataset()

with atp_rates_dataset.get_writer() as writer:
    for match in my_gen:
        print('MATCH WRITEN                -----------')
        print(match)
        writer.write_row_dict(match)
