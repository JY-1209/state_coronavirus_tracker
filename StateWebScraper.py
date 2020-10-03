# Default Python Libraries
import time
import sys
import os

# External Python Libraries
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
options = Options()
options.headless = True #see headless browser
import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
import re
import urllib
import urllib.request
from urllib.parse import urlparse

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

import time
import multiprocessing
from search_engine_parser.core.engines.google import Search as GoogleSearch
from googleapiclient.discovery import build
import datetime
import requests

import logging
from selenium.webdriver.remote.remote_connection import LOGGER
LOGGER.setLevel(logging.WARNING)

# Creation of the phantomjs webdriver
driver = webdriver.PhantomJS('Drivers/phantomjs.exe')

# Dataset Optimization
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
normal_districts = pd.read_csv("CSV_Files/state_information.csv")

# CSV file for writing and viewing the results of CafrScraper.py
state_results = pd.read_csv("CSV_Files/state_results.csv")

# Data Preprocessing
normal_districts.dropna()

state_results['coronavirus_numbers'] = state_results['coronavirus_numbers'].astype('object')
# state_results['county_website'] = state_results['county_website'].astype('object')
# state_results['county_name'] = state_results['county_name'].astype('object')

# External Dataset Variables for below for loop. Switch variables when using a different csv file
website_dataset = normal_districts
website_csv = "CSV_Files/state_information.csv"

# Gives the code a break every 50 search queries to prevent google search engine failure from suspicious activity
count = 0
# The total nummber of counties that have been been iterated. NOTE: the county associaed with the total variable is -3 from the county in the excel file
total = 0

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
headers = {"user-agent": USER_AGENT,
            'Host': 'www.google.com',
            'Referer': 'https://www.google.com/'}

first_state_set = {"Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", 
                   "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", 
                   "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", 
                   "Minnesota", "Mississippi"}

second_state_set = {"Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", 
                   "New York", "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", 
                   "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont", 
                   "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"}

state_number = {"Alabama" : 0, "Alaska": 1, "Arizona": 2, "Arkansas" : 3, "California" : 4, "Colorado": 5, "Connecticut": 6, 
                   "Delaware": 7, "Florida": 8, "Georgia": 9, "Hawaii": 10, "Idaho": 11, "Illinois": 12, "Indiana": 13, "Iowa": 14, 
                   "Kansas": 15, "Kentucky": 16, "Louisiana": 17, "Maine": 18, "Maryland": 19, "Massachusetts": 20, "Michigan": 21, 
                   "Minnesota": 22, "Mississippi": 23, "Missouri": 24, "Montana": 25, "Nebraska": 26, "Nevada": 27, "New Hampshire": 28,
                   "New Jersey": 29, "New Mexico": 30, "New York": 31, "North Carolina": 32, "North Dakota": 33, "Ohio": 34, 
                   "Oklahoma": 35, "Oregon": 36, "Pennsylvania": 37, "Rhode Island": 38, "South Carolina": 39, "South Dakota": 40, 
                   "Tennessee": 41, "Texas": 42, "Utah": 43, "Vermont": 44, "Virginia": 45, "Washington": 46, "West Virginia": 47, 
                   "Wisconsin": 48, "Wyoming": 49}

coronavirus_dict = {}

# multi-threading for making at max 25 requests
def multi_retrieve_corona_data():
    # Execute our get_data in multiple threads each having a different page number
    MAX_THREADS = 25
    
    for counter in range(0, 2):
        if counter == 0:
            state_set = first_state_set
        else:
            state_set = second_state_set

        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            future_to_url = {executor.submit(load_url, state): state for state in state_set}
            for future in concurrent.futures.as_completed(future_to_url):
                # print("FUTURE_RESULT: " + str(future.result()))
                data = future.result()
                print(f"data: {data}")
                coronavirus_dict[data["state"]] = data

# Retrieve a single page and report the URL and contents
def load_url(state):
    search_term = f"{state} coronavirus"
    query = search_term.replace(' ', '+')
    URL = f"https://www.google.com/search?num=10&start=0&q={query}&client=ubuntu"  
    print(f"Accessing URL: {URL}")   
    request = urllib.request.Request(URL,None,headers) #The assembled reques
    response = urllib.request.urlopen(request)
    soup = BeautifulSoup(response.read().decode('utf-8'), "html.parser")

    for element in soup.find_all('div', class_='wveNAf'):
        if state.lower() in element.text.lower():
            corona_web_element = element.find('td', class_='dZdtsb QmWbpe ZDeom')
            corona_infected = int(corona_web_element.get('data-vfs'))
            corona_infected = format (corona_infected, ',d')
            print(f"Number infected with Corona: {corona_infected}")

            if (corona_web_element.find('div', 'h5Hgwe') is not None):
                daily_change_element = corona_web_element.find('div', 'h5Hgwe')
                corona_daily_change = daily_change_element.find('span').text
            else:
                corona_daily_change = "N/A"

            print(f"Corona daily change: {corona_daily_change}\n")
            corona_dict = {}
            corona_dict["state"] = state
            corona_dict['corona_infected'] = corona_infected
            corona_dict['corona_daily_change'] = corona_daily_change
            return corona_dict

def save_CSV():
    for index, row in website_dataset.iterrows():
        state = row['state']
        state_results.at[state_number[state], "coronavirus_numbers"] = coronavirus_dict[state]["corona_infected"]
        state_results.at[state_number[state], "daily_change"] = coronavirus_dict[state]["corona_daily_change"]
    state_results.to_csv("CSV_Files/state_results.csv", encoding='utf-8', index=None)

multi_retrieve_corona_data()
save_CSV()
# genesis = time.time()
# for index, row in website_dataset.iterrows():
#     # 1570c
#     if (total <= -1):
#         total += 1
#         continue
#     # Ending point of the code
#     if (total > 10):
#         print("time for 50 states: ", time.time() - genesis, " seconds")
#         print("END OF SCRIPT")
#         break
#     # Code timeout to avoid suspicious activity detection
#     if count == 50:
#         count = 0
#         time.sleep(10)
#         print("TIMEOUT 10")

#     state = row['state']

#     # Creates/Resets the visited websites:
#     visited_websites = set()

#     search_term = f"{state} coronavirus"

#     print("=============================SEARCHING: " + '"' + search_term + '"=============================')

#     USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
#     headers = {"user-agent": USER_AGENT,
#                'Host': 'www.google.com',
#                'Referer': 'https://www.google.com/'}

#     query = search_term.replace(' ', '+')
#     URL = f"https://www.google.com/search?num=10&start=0&q={query}&client=ubuntu"  
#     print(f"Accessing URL: {URL}")   
#     request = urllib.request.Request(URL,None,headers) #The assembled reques
#     response = urllib.request.urlopen(request)
#     soup = BeautifulSoup(response.read().decode('utf-8'), "html.parser")

#     # for element in soup.find_all('div', class_='wveNAf'):
#     #     if state.lower() in element.text.lower():
#     #         corona_web_element = element.find('td', class_='dZdtsb QmWbpe ZDeom')
#     #         corona_infected = int(corona_web_element.get('data-vfs'))
#     #         corona_infected = format (corona_infected, ',d')
#     #         print(f"Number infected with Corona: {corona_infected}")
#     #         state_results.at[total, "coronavirus_numbers"] = corona_infected

#     #         if (corona_web_element.find('div', 'h5Hgwe') is not None):
#     #             daily_change_element = corona_web_element.find('div', 'h5Hgwe')
#     #             corona_daily_change = daily_change_element.find('span').text
#     #         else:
#     #             corona_daily_change = "N/A"
#     #         print(f"Corona daily change: {corona_daily_change}\n")
#     #         state_results.at[total, "daily_change"] = corona_daily_change

#     #         total += 1
#     #         state_results.to_csv("CSV_Files/state_results.csv", encoding='utf-8', index=None)
#     multi_retrieve_corona_data()
    
#     time.sleep(1)   









