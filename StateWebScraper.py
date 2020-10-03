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

genesis = time.time()
for index, row in website_dataset.iterrows():
    # 1570c
    if (total <= -1):
        total += 1
        continue
    # Ending point of the code
    if (total > 10):
        print("time for 50 states: ", time.time() - genesis, " seconds")
        print("END OF SCRIPT")
        break
    # Code timeout to avoid suspicious activity detection
    if count == 50:
        count = 0
        time.sleep(10)
        print("TIMEOUT 10")

    state = row['state']

    # Creates/Resets the visited websites:
    visited_websites = set()

    search_term = f"{state} coronavirus"

    print("=============================SEARCHING: " + '"' + search_term + '"=============================')

    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
    headers = {"user-agent": USER_AGENT,
               'Host': 'www.google.com',
               'Referer': 'https://www.google.com/'}

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
            state_results.at[total, "coronavirus_numbers"] = corona_infected

            if (corona_web_element.find('div', 'h5Hgwe') is not None):
                daily_change_element = corona_web_element.find('div', 'h5Hgwe')
                corona_daily_change = daily_change_element.find('span').text
            else:
                corona_daily_change = "N/A"
            print(f"Corona daily change: {corona_daily_change}\n")
            state_results.at[total, "daily_change"] = corona_daily_change

            total += 1
            state_results.to_csv("CSV_Files/state_results.csv", encoding='utf-8', index=None)
    
    time.sleep(1)   









