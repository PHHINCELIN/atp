#-*- coding: utf-8 -*-
"""Scrap Tennis players weekly ranks on ATP.
"""
import requests
from bs4 import BeautifulSoup
import csv
import time
from retrying import retry
import random
import datetime
from selenium import webdriver
from fake_useragent import UserAgent
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem, HardwareType, SoftwareType, Popularity


def decorator_counter(func):
    """Decorate a function and count the number of times a function is used."""
    def wrapper(*args, **kargs):
        wrapper.count += 1
        res = func(*args, **kargs)
        return res
    wrapper.count = 0
    return wrapper


def decorator_exec_time(func):
    """Decorate a function and compute total exec time."""
    def wrapper(*args, **kargs):
        start_time = time.time()
        res = func(*args, **kargs)
        duration = time.time() - start_time
        print('It took {0} seconds'.format(duration))
        return res
    return wrapper


def sleeper(alpha, beta):
    """Stop execution for a random duration."""
    time.sleep(random.gammavariate(alpha,beta))
    

@decorator_counter
@retry(stop_max_attempt_number=3, wait_exponential_multiplier=500, wait_exponential_max=1500)
def get_soup(page):
    """Get soup from an HTML page."""
    #ua = UserAgent()
    #userAgent = ua.random
    chrome_options = webdriver.ChromeOptions()#Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1200")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--enable-javascript")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument('user-agent={0}'.format(get_browser()))
    driver = webdriver.Chrome(options=chrome_options, service_args=["--verbose", "--log-path=/tmp/qc1.log"])
    print('BOOM MAKING THE SOUP for {0}'.format(page))

    driver.get(page)
    #html_page = requests.get(page, timeout=1).content
    #soup = BeautifulSoup(html_page, 'lxml')
    soup = BeautifulSoup(driver.page_source)
    print('BOOM GETTING THE SOUP for {0}'.format(page))
    driver.quit()
    return soup


def check_date(date, date_start, date_stop):
    """Check if date is in the intervall [date_start ; date_stop]
    
    date_start (eg : Jan 1st, 2000) < date_stop (eg : Jan 10th, 2016)
    all variables are datetime object
    """
    timedelta_start = date - date_start
    timedelta_stop = date - date_stop
    if timedelta_start.days >=0:
        # date is most recent that date_start
        if timedelta_stop.days <=0:
            #date is older than date_stop
            return True
        else :
            return False
    else:
        return False


def validate_date(date_text):
    """Check if input date has the correct format 'YYYY-mm-dd' and convert it to a date object"""
    try:
        return datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")
        
def get_browser():
    """Get a random user agent from random_user_agent package"""
    software_names = [SoftwareName.EDGE.value, SoftwareName.CHROMIUM.value, SoftwareName.FIREFOX.value, SoftwareName.CHROME.value ]
    operating_systems = [OperatingSystem.UNIX.value, OperatingSystem.ANDROID.value, OperatingSystem.CHROMEOS.value, OperatingSystem.FIRE_OS.value, OperatingSystem.FREEBSD.value, OperatingSystem.IOS.value, OperatingSystem.LINUX.value, OperatingSystem.MAC.value, OperatingSystem.MAC_OS_X.value, OperatingSystem.MACOS.value, OperatingSystem.OPENBSD.value, OperatingSystem.WINDOWS.value, OperatingSystem.WINDOWS_MOBILE.value, OperatingSystem.WINDOWS_PHONE.value] 
    hardware_types = [HardwareType.COMPUTER.value, HardwareType.MOBILE.value, HardwareType.MOBILE__PHONE.value]
    software_types = [SoftwareType.WEB_BROWSER.value]
    popularity = [Popularity.POPULAR.value, Popularity.COMMON.value]

    user_agent_rotator = UserAgent(operating_systems=operating_systems, hardware_types=hardware_types, popularity=popularity)

    return user_agent_rotator.get_random_user_agent()


@decorator_counter
@retry(stop_max_attempt_number=3, wait_exponential_multiplier=500, wait_exponential_max=1500)
def get_soup2(page, driver):
    """Get soup from an HTML page."""
    print('BOOM MAKING THE SOUP for {0}'.format(page))

    driver.get(page)
    #html_page = requests.get(page, timeout=1).content
    #soup = BeautifulSoup(html_page, 'lxml')
    soup = BeautifulSoup(driver.page_source)
    print('BOOM GETTING THE SOUP for {0}'.format(page))
    return soup

def create_driver():
    """create a chrome driver"""
    chrome_options = webdriver.ChromeOptions()#Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('user-agent={0}'.format(get_browser()))
    chrome_options.add_argument("--disable-javascript")
    driver = webdriver.Chrome(options=chrome_options, service_args=["--verbose", "--log-path=/tmp/qc1.log"])
    return driver

def delete_driver(driver):
    """delete a chrome driver"""
    driver.quit()