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
  path = os.path.abspath('dwh_pipelines/0_src_data_generator/config.ini')
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



  NO_CUSTOMER_INFO_ROWS = 100




  # ========================================================================================================================================================================

  # ============================ CUSTOMER INFORMATION ============================

   # Create a Faker instance to generate fake data
  fake = Faker()


  customer_info_records = []

  for i in range(NO_CUSTOMER_INFO_ROWS):
    customer_id = uuid.uuid4(),
    first_name = fake.first_name(),
    last_name = fake.last_name(),
    email = f"{first_name.lower()}.{last_name.lower()}@" + random.choice(['email.com', 'inlook.com', 'mzn.com', 'aio.net', 'macrosoft.com' ]),
    place_of_birth = fake.city(),
    dob = fake.date_of_birth(),
    today = datetime.today(),
    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day)),
    address = fake.address(),
    city = fake.city(),
    state = fake.state(),
    zip = fake.zip(),
    phone = fake.phone_number(),
    country = fake.country(),
    credit_card = fake.credit_card_number(),
    credit_card_provider = fake.credit_card_provider(),
    nationality = fake.country(),
    customer_info_records.append({
      'customer_id': customer_id,
      'first_name': first_name,
      'last_name': last_name,
      'email': email,
      'place_of_birth': place_of_birth,
      'age': age,
      'address': address,
      'city': city,
      'state': state,
      'zip': zip,
      'phone': phone,
      'credit_card': credit_card,
      'credit_card_provider': credit_card_provider,
      'nationality': nationality,
      'country': country

    })


  customer_info_df = pd.DataFrame(customer_info_records)


   # # Write dataframe to JSON file 
  with open(f'{DATASETS_LOCATION_PATH}/customer_info.json', 'w') as customer_info_file:
    customer_info_df_to_json = customer_info_df.to_json(orient="records")
    customer_info_file.write(json.dumps(json.loads(customer_info_df_to_json), indent=4, sort_keys=True)) 


  # Print the customer information title in console
  print('----------')
  print('============================ CUSTOMER INFORMATION ============================')
  print(customer_info_df)
  





