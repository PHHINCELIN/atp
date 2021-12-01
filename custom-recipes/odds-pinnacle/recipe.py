# Imports
import dataiku
import csv
import datetime
import os
import errno
import encodings
import time
from selenium import webdriver
import datetime


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
PINNACLE_URL = "https://www.pinnacle.com/fr/odds/today/tennis"


def gen_pinnacle_tennis_odds(driver):
    try:
        elements = driver.find_elements_by_xpath('//*[@data-market]')
    except Exception as e:
        print("Fail while extracting information from HTML page")
    data_markets = []
    for element in elements:
        data_markets.append(element.get_attribute('data-market'))
        data_markets =list(set(data_markets))

    # Getting the matches details for each ongoing tourney
    for tournament in data_markets:

        try:
            matches = driver.find_elements_by_xpath("//*[@data-market='"+ tournament +"']/*/tbody")
        except Exception as e:
            print("Fail while extracting information from HTML page")
        for m in matches:
            match = {}

            lines = m.find_elements_by_tag_name('tr')
            try:
                match['tournament-name-round'] = driver.find_element_by_xpath("//*[@data-market='"+ tournament +"']/*/span").text
                match['player1_name'] = lines[0].find_element_by_class_name("game-name").text
                match['player1_odd'] = lines[0].find_element_by_class_name("game-moneyline").text
                match['player2_name'] = lines[1].find_element_by_class_name("game-name").text
                match['player2_odd'] = lines[1].find_element_by_class_name("game-moneyline").text
                match['date'] = datetime.datetime.now()
            except Exception as e:
                print("Fail while extracting information from HTML page")
            
            yield match
            



# Recipe outputs
atp_rates_dataset = output_dataset[0]
atp_rates_dataset.write_schema([
{"name": "date",
"type": "string"
},
{"name": "tournament-name-round",
"type": "string"
},
{"name": "player1_name",
"type": "string"
},
{"name": "player1_odd",
"type": "string"
},
{"name": "player2_name",
"type": "string"
},
{"name": "player2_odd",
"type": "string"
}
])


# Start the driver
driver = webdriver.PhantomJS()
driver.get(PINNACLE_URL)

my_gen = gen_pinnacle_tennis_odds(driver)

with atp_rates_dataset.get_writer() as writer:
    for match in my_gen:
        print('MATCH WRITEN                -----------')
        print(match)
        writer.write_row_dict(match)

driver.quit()