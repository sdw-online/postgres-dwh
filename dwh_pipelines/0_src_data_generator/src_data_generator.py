import pandas as pd
import random
import uuid
import string
import json
from faker import Faker
from datetime import datetime
import configparser
import os 


def generate_travel_data():

  # Establish the relevant constants for generating the synthetic travel data  
  config = configparser.ConfigParser()
  path = os.path.abspath('batch_pipelines/dags/data_generators/config.ini')
  config.read(path)

  DATASETS_LOCATION_PATH = config['travel_data_filepaths']['DATASETS_LOCATION_PATH']


  # Update the DATASETS_LOCATION_PATH variable
  # config['travel_data_filepaths']['DATASETS_LOCATION_PATH'] = '/usr/local/airflow/datasets'


  print('------------------------------------------------')
  print('------------------------------------------------')
  print(f'DATASET DESTINATION PATH: {DATASETS_LOCATION_PATH} ')
  print('------------------------------------------------')
  print('------------------------------------------------')


  NUM_TRAVEL_OPTIONS = 30
  NUM_CUSTOMERS = 1500
  NUM_FX_RATES = 800
  NUM_TRAVEL_BOOKINGS = 200
  NUM_SALES = 20000
  NUM_SALES_AGENTS = 65
  NUM_ACCOMMODATION_BOOKINGS = 20000
  DESTINATIONS = ['Paris', 'Rome', 'Barcelona', 'Amsterdam', 'London', 'New York', 'Sydney', 'Bali', 'Maldives', 'Tokyo',
                  'Dubai', 'Bangkok', 'Istanbul', 'Cancun', 'Phuket', 'Bora Bora', 'Santorini', 'Cancun', 
                  'Maui', 'Serengeti', 'Venice', 'Prague', 'Maui']
  INCLUSIONS = ['Accommodation', 'Breakfast', 'Lunch', 'Dinner', 'Transportation', 'Guided tours', 'Entrance fees', 'Sightseeing', 'Airport transfers', 'Cruise']
  EXCLUSIONS = ['Flights', 'Visas', 'Insurance', 'Personal expenses', 'Optional activities', 'Tips and gratuities', 'Extra meals', 'Beverages', 'Laundry services', 'Phone calls']
  CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'DKK', 'HKD', 'NOK', 'SEK', 'SGD', 'THB', 'TWD', 'ZAR']


  # ========================================================================================================================================================================

  # ============================ CUSTOMER INFORMATION ============================