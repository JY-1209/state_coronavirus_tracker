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
normal_districts = pd.read_csv("CSV_Files/Company_CSV.csv")

# CSV file for writing and viewing the results of CafrScraper.py
county_report = pd.read_csv("CSV_Files/Company_Finances.csv")

# Scrapes all the A tags in a layer and if cafr files are found, return the files. If not, store the filtered hrefs for another layer
def layer_scraper(retrieved_hrefs, loc, scraping_layer, visited_websites):
    print("websites to be scraped: " + str(retrieved_hrefs) + "\n")
    social_media = {'facebook', 'linkedin', 'translate.google', 'instagram', 'plus.google', 'books.google', "twitter"}

    scraped_cafrs = []
    dict_result = {}
    scraped_hrefs = []
    for href in retrieved_hrefs:
        layer_scraped_hrefs = []
        Beginning_time = time.time()
        href_lower = href.lower()

        skip = False
        # ignores websites that are social media sites
        for s_c in social_media:
            if s_c in href_lower:
                skip = True
                continue
        if (skip):
            continue
        url = href

        if (url in visited_websites):
            continue
        else:
            visited_websites.add(url)

        print("The url grabbed is: " + url)
        start_time = time.time()

        try:
            driver.get(href)
        except Exception as ex:
            print("aborting driver")
            driver.execute_script("window.stop();")
        
        javascript_enabled = False

        print("driver.get(): ", time.time() - start_time, " seconds")
        start_time = time.time()

        # Try-Except statements are used to run the javascript on the webpage
        time.sleep(1)
        try:
            innerHTML = driver.execute_script("return document.documentElement.innerHTML")
            print("javascript execution: ", time.time() - start_time, " seconds")
            start_time = time.time()
            time.sleep(1)
            title = BeautifulSoup(innerHTML, 'lxml', parse_only=SoupStrainer('title')).find('title')
            javascript_enabled = True
        except Exception as ex:
            print("exception: " + str(ex))
            title = BeautifulSoup(driver.page_source, 'lxml', parse_only=SoupStrainer('title')).find('title')

        print("javascript parse: ", time.time() - start_time, " seconds")
        start_time = time.time()
        # Removes and returns web pages if they have school/education in the title of the webpage
        if (title != None): 
            if (title.string != None):
                if ("school" in title.string.lower() or "education" in title.string.lower()):
                    print("has school/education")
                    print("scraped_hrefs" + str(scraped_hrefs))
                    dict_result["scraped_hrefs"] = scraped_hrefs
                    dict_result["visited_websites"] = visited_websites
                    return dict_result

        # Initalizes a list to contain all the a-tags with hrefs on the webpage
        if javascript_enabled:
            soup = BeautifulSoup(innerHTML, 'lxml', parse_only=SoupStrainer('a', href=True))
        else:
            soup = BeautifulSoup(driver.page_source, 'lxml', parse_only=SoupStrainer('a', href=True))
        
        print("soup parse: ", time.time() - start_time, " seconds")
        start_time = time.time()

        domain = urlparse(url).netloc
        a_tags = soup.find_all('a', href=True)
        key_links = []

        start_time = time.time()
        count = 0 # useless code only for check_href_is_cafr
        for a_tag in a_tags:
            a_tag_url = ""
            href_text = str(a_tag.get('href'))
            if len(href_text) < 1:
                continue
            # formats the a-tag href into a working url
            if 'mailto:' in href_text:
                continue

            skip = False
            # ignores websites that are social media sites
            for s_c in social_media:
                if s_c in href_lower:
                    skip = True
                    continue
            if (skip):
                continue

            if domain.lower() in href_text.lower():
                if href_text.lower() != url.lower():
                    a_tag_url = (a_tag.get('href'))
                else:
                    continue
            else:
                if domain.lower() + a_tag.get('href').lower() != url[url.index('//') + 2:].lower():
                    if a_tag.get('href')[0] != "/":
                        a_tag_url = (url[:url.index('//') + 2] + domain + "/" + href_text)
                    else:
                        a_tag_url = (url[:url.index('//') + 2] + domain + href_text)

            if (a_tag_url in visited_websites or a_tag_url == ""):
                continue
            cafr_link = check_if_href_has_keys(a_tag, a_tag_url)  # will return a list in the format ['cafr website','cafr a_tag string']
            if (cafr_link != None):
                is_cafr = check_href_is_cafr(cafr_link[1], count)
                count += 1
                if (is_cafr[0]):  # returns true or false
                    cafr_info = []
                    cafr_info.append(cafr_link[0])
                    cafr_info.append(is_cafr[1])
                    key_links.append(cafr_info)
                else:
                    scraped_hrefs.append(cafr_link[0])
                    layer_scraped_hrefs.append(cafr_link[0])
            else:
                continue 

        counter = 0    
        urls_multithread = []
        print("checking all a_tags: ", time.time() - start_time, " seconds")
        start_time = time.time()

        # starts the multi-threading every 10 a_tags
        for key_link in key_links:
            counter += 1
            urls_multithread.append(key_link)

            if (counter % 40 == 0 or counter == len(key_links)):
                page_types = multi_get_data(urls_multithread)
                for page in page_types:
                    # page will be a list. [0] - a_tag url, [1] - content type
                    if page[2] is None:
                        continue
                    if ("application/pdf" in page[2].lower()):
                        scraped_cafrs.append(page[0:2])
                        # print("page: " + str(page))
                    elif ('error' not in page[2].lower()):
                        if (page[0].lower() not in scraped_hrefs):
                            scraped_hrefs.append(page[0])
                            layer_scraped_hrefs.append(page[0])

                urls_multithread.clear()
        
        print("multithread: ", time.time() - start_time, " seconds")
        print("total_time: ", time.time() - Beginning_time, " seconds")
        if (len(scraped_cafrs) > 0):
            print()
            print("layer_scraped_cafrs: " + str(scraped_cafrs))
            dict_result["scraped_cafrs"] = scraped_cafrs
            dict_result["visited_websites"] = visited_websites
            dict_result["scraped_hrefs"] = layer_scraped_hrefs
            return dict_result        

    print("scraped_hrefs" + str(scraped_hrefs))
    dict_result["scraped_hrefs"] = scraped_hrefs
    dict_result["visited_websites"] = visited_websites
    return dict_result

# Checks if a href has the necessary key words
def check_if_href_has_keys(a_tag, test_url):
    key_words = {"cafr", "comprehensiveannualfinancialreport", "financialstatement", "financialreport", "finance", "audit", "annualreport",
                 "comprehensiveannualfinancial", "fiscalyear", "financialinformation"}
    info = []
    bad_words = {'popular', 'budget', 'pafr', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october'
                 'november', 'december', 'facebook', 'linkedin', 'translate.google', 'instagram', 'plus.google', 'books.google', 'pending',
                 'month', 'dnb.com', 'reddit.com'}

    for bad_word in bad_words:
        if bad_word in str(a_tag).lower():
            return

    text = re.sub('[^A-Za-z0-9]+', '', str(a_tag).lower())
    for key_word in key_words:
        if key_word in text:
            info.append(test_url)
            info.append(str(a_tag).lower())
            return info
    return

def check_href_is_cafr(text, count):
    href_info = []
    href_info.append(False)
    specific_key_words = {"cafr", "financialreport", "comprehensiveannualfinancial", "audit", "annualreport", 'financialstatement',
                          'fiscalyear'}
    possible_years = re.findall(r"[0-9]{4}", text)
    
    regex_text = re.sub('[^A-Za-z0-9]+', '', str(text).lower())
    for specific_key_word in specific_key_words:
        if specific_key_word in regex_text:
            href_info[0] = True

    years = set()
    for possible_year in possible_years:
        if 1970 <= int(possible_year) and int(possible_year) <= datetime.datetime.now().year:
            years.add(possible_year)

    if len(years) > 0:
        href_info.append(min(years) + " " + str(count)) 

    if len(href_info) < 2:
        href_info.append(str(count))

    return href_info

def createFolder(folderName):
    directory = './JustinPDF/' + folderName + '/'
    # directory='./shaoqi_PDFs/'+folderName+'/'
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print('Error: Creating directory. ' + directory)

# multi-threading for making at max 10 requests
def multi_get_data(urls):
    # Execute our get_data in multiple threads each having a different page number
    content_type_results = []
    MAX_THREADS = 40
    urls_length = len(urls)
    if urls_length < 1:
        urls_length = 1
    threads = min(MAX_THREADS, urls_length)
    with ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_url = {executor.submit(load_url, url): url for url in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            # print("FUTURE_RESULT: " + str(future.result()))
            url = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (url, exc))
                content_type_results.append("no content-type page found due to website not being written in html")
            else:
                # print('%r page is %d bytes' % (url, len(data)))
                content_type_results.append(data)
    return content_type_results

# Retrieve a single page and report the URL and contents
def load_url(test_url):
    # print(test_url[0])
    if test_url[-4:] == ".pdf":
        test_url.append('application/pdf')
        return test_url

    try:
        a_tag_url = test_url[0]  # the A tag href
        user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
        headers={'User-Agent':user_agent,
                 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',}
        request = urllib.request.Request(a_tag_url,None,headers) #The assembled reques
        response = urllib.request.urlopen(request, timeout=30)
        r = response.getheader('content-type')
        time.sleep(1)
        test_url.append(r)
        return test_url
    # print("request: " + r.headers['Content-Type'])
    except Exception as ex:
        # print("error: " + str(ex))
        test_url.append(str(ex))
        return test_url

def download_cafrs(cafr_info, path):
    # Execute our get_data in multiple threads each having a different page number
    MAX_THREADS = 20
    length = len(cafr_info)
    if length < 1:
        length = 1
    threads = min(MAX_THREADS, length)
    with ThreadPoolExecutor(max_workers=threads) as executor:
        {executor.submit(download_cafr, key, cafr_info[key], path): key for key in cafr_info.keys()}

def download_cafr(cafr_link, value, path):
    try:
        path += value + ".pdf"
        # print("path: " + path)
        # print("cafr_link: " + cafr_link)
        pdf = requests.get(cafr_link, timeout = 10)
        with open(path, 'wb') as f:
            f.write(pdf.content)
    except Exception as ex:
        print("error in downloading: " + str(ex))

def remove_chars(text): # removes numeric characters and spaces from a string
    text = re.sub('[^A-Za-z0-9]+', '', text)
    pattern = '[0-9]'
    text = re.sub(pattern, '', text)
    return text

# Data Preprocessing
normal_districts.dropna()

county_report['county_cafrs'] = county_report['county_cafrs'].astype('object')
county_report['county_website'] = county_report['county_website'].astype('object')
county_report['county_name'] = county_report['county_name'].astype('object')

# dictionary of the counties and their cafrs
cafrs_dict = {}
# dictionary of the counties and their websites which could potentially lead to cafrs
cafr_counties = {}
# a list of the counties that have been checked
county_list = []

# External Dataset Variables for below for loop. Switch variables when using a different csv file
website_dataset = normal_districts
website_csv = "CSV_Files/Company_CSV.csv"

# Gives the code a break every 50 search queries to prevent google search engine failure from suspicious activity
count = 0
# The total nummber of counties that have been been iterated. NOTE: the county associaed with the total variable is -3 from the county in the excel file
total = 0

# directory_path = input("Enter the directory path: ") + "\\"
directory_path = "C:\\Users\\justi\\OneDrive\\!Professional Work\\Bond Intelligence\\SearchScrapeCafrs\\JustinPDF\\"
gensis = time.time()
for index, row in website_dataset.iterrows():
    # 1570c
    if (total <= -1):
        total += 1
        continue
    # Ending point of the code
    if (total > 5):
        print("time for 100 cafrs: ", time.time() - gensis, " seconds")
        print("END OF SCRIPT")
        break
    # Code timeout to avoid suspicious activity detection
    if count == 50:
        count=0
        time.sleep(10)
        print("TIMEOUT 10")

    parent_county = "county of " + row['COUNTY_NAME'].lower()
    # County name modifications
    county = row['COUNTY_AREA_NAME'].lower()
    county_list.append(county.lower())
    
    county_report.at[total, "county_name"] = county.lower()
    # eliminates the type of the location (county, town, city) and 
    county_split_list = county.split()
    county_name = ""
    for split_county in range(2, len(county_split_list)):
        if split_county > 2:
            countyname = countyname + " " + county_split_list[split_county]
        else:
            countyname = county_split_list[split_county]

    county_name = county[county.find("of") + 3:]
    county_first_name = county_name.split()[0]
    county_name = countyname.replace(" ", "")

    state = row['STATE_AB']

    # Creates/Resets the visited websites:
    visited_websites = set()

    search_term = str(county) + " \"" + str(state) + "\" " + "financial report"
    retrieved_websites_list = []

    print("SEARCHING: " + '"' + search_term + '"')
    print(str(count)+ "/100" + ", report number: " + str(total))
    # the filtered/end product urls
    selected_cafr_websites = []

    try: 
        search_args = (search_term, 0)
        gsearch = GoogleSearch()
        gresults = gsearch.search(*search_args, cache=True)
        for num in range(0, 7):
            retrieved_websites_list.append(gresults[num]['link'])
    except Exception as ex:
        print("INVALID SEARCH RESULTS (most likely due to an image search result)")
        USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"
        headers = {"user-agent": USER_AGENT,
                'Host': 'www.google.com',
                'Referer': 'https://www.google.com/'}

        query = search_term.replace(' ', '+')
        URL = f"https://www.google.com/search?num=10&start=0&q={query}&client=ubuntu"     
        request = urllib.request.Request(URL,None,headers) #The assembled reques
        response = urllib.request.urlopen(request)
        soup = BeautifulSoup(response.read().decode('utf-8'), "html.parser")

        count = 0
        for g in soup.find_all('div', class_='r'):
            if (count == 7):
                break
            anchors = g.find_all('a')
            if anchors:
                link = anchors[0]['href']
                title = g.find('h3').text
                retrieved_websites_list.append(link)
                count += 1
                
    print(f"Retrieved_websites: {retrieved_websites_list}")
    # prints the full list of the top 5 urls displayed
    # print("Google Web Results: " + '[%s]' % ', '.join(map(str, retrieved_websites_list)))
    key_cafr_phrases = ["cafr|comprehensiveannualfinancialreport", "finan", "finance", "audit",
                        "document", "comprehensiveannualfinancial", "budgetreports", "financialinformation"]

    cafr_websites = []
    for website in retrieved_websites_list:
        # checks to see if the cafr is a pdf or not
        if "application/pdf" in website.lower() or "salary" in website.lower() or "school" in website.lower() or "education" in website.lower():
            continue
        
        if "timed out" in website.lower():
            continue
        
        # eliminates the websites that don't have the county name in them
        has_name = False

        text = remove_chars(website.lower())
        regex_search = re.search(county_name, text)
        if (regex_search != None):
            has_name = True
    
        # checks to make if there is a key_cafr_phrase in the url
        has_cafr_phrase = False
        for key_cafr_phrase in key_cafr_phrases:
            regex_search = re.search(key_cafr_phrase, text)
            if (regex_search != None):
                has_cafr_phrase = True
                break

        if (has_name and has_cafr_phrase):
            cafr_websites
            key_website = []
            key_website.append(website)
            cafr_websites.append(key_website)

    if len(cafr_websites) > 0:
        content_types = multi_get_data(cafr_websites)
    else:
        print("NO FILTERED WEBSITES WERE FOUND")
        count +=1
        total += 1
        print()
        continue
    print()    
    
    for content_type in content_types:
        # page will be a list. [0] - a_tag url, [1] - content type
        # print("content_type: " + str(content_type))
        if ("application/pdf" not in content_type[1].lower() and "error" not in content_type[1].lower()):
            selected_cafr_websites.append(content_type[0])

    if (len(selected_cafr_websites) >= 1):
        cafr_counties[county] = selected_cafr_websites
        county_report.at[total, "county_website"] = cafr_counties[county]
    else:
        county_report.at[total, "county_website"] = ""

    print("Filtered Websites to be Searched: " + '[%s]' % ', '.join(map(str, selected_cafr_websites)))

        
    cafrs_collection = {}
    # if any websites were retrieved
    if (len(selected_cafr_websites) > 0):
        print("STARTING THE CAFR SEARCH...")

        # implements layer scraping
        for iteration in range(1, 3):
            if (len(selected_cafr_websites) > 0):
                print("\nSCRAPING LAYER " + str(iteration) + "...")
                scraped_layer = layer_scraper(selected_cafr_websites, county_name.lower(), iteration, visited_websites)
                visited_websites = scraped_layer["visited_websites"]
            else:
                break

            if "scraped_cafrs" in scraped_layer.keys():
                for scraped_cafr in scraped_layer["scraped_cafrs"]:
                    if (scraped_cafr[0] not in cafrs_collection):
                        cafrs_collection[scraped_cafr[0]] = scraped_cafr[1]

            retrieved_websites = scraped_layer["scraped_hrefs"]

            for selected_cafr_website in selected_cafr_websites:
                if (selected_cafr_website in retrieved_websites):
                    retrieved_websites.remove(selected_cafr_website)
            
            selected_cafr_websites = retrieved_websites
        
        print("\nTOTAL COUNTY SCRAPED CAFRS: " + str(cafrs_collection))
        county_report.at[total, "county_cafrs"] = cafrs_collection

        # downloading code:
        if (len(cafrs_collection) > 0):
            cafrs_dict = cafrs_collection
        else:
            county_report.at[total, "county_cafrs"] = ""
            print("No cafrs were found.")

    county_report.at[total, "county_cafrs"] = cafrs_dict.keys()
    print()

    count += 1
    total += 1

    # print(county_report.county_name.to_string(index=False))
    county_report.to_csv("CSV_Files/Company_Finances.csv", encoding='utf-8', index=None)
    time.sleep(1)

print()
print("county_list: " + '[%s]' % ', '.join(map(str, county_list)))
print() 
print("cafr_dict: " + str(cafrs_dict))
print() 
print("cafr_counties: " + str(cafr_counties))
print()









