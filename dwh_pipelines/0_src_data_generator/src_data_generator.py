import pandas as pd
import random
import uuid
import string
import json
from faker import Faker
from datetime import datetime, timedelta
import configparser
import os 


def generate_travel_data():

  # Establish the relevant constants for generating the synthetic travel data  
  config = configparser.ConfigParser()
  path = os.path.abspath('dwh_pipelines/local_config.ini')
  config.read(path)

  DATASETS_LOCATION_PATH = config['travel_data_filepath']['DATASETS_LOCATION_PATH']


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



  NO_OF_CUSTOMER_INFO_ROWS = 100
  NO_OF_FLIGHT_SCHEDULES = 100




  # ========================================================================================================================================================================

  # ============================ CUSTOMER INFORMATION ============================

   # Create a Faker instance to generate fake data
  fake = Faker()


  random_date = random.choice(pd.date_range(start='2012-01-01', end='2022-12-31'))
  created_date = datetime.strptime(str(random_date.date()), "%Y-%m-%d")
  print(created_date)
  print(timedelta(days=random.randint(1, 40)))
  created_date = pd.Timedelta(days=random.randint(1, 40))
  # created_date = datetime.strptime(str(random_date.date()), "%Y-%m-%d")
  
  preferred_contact_method = [
  {"id": uuid.uuid4(), "description": "Email"},
  {"id": uuid.uuid4(), "description": "Phone"},
  {"id": uuid.uuid4(), "description": "Text message"},
  {"id": uuid.uuid4(), "description": "Postal mail"},
  {"id": uuid.uuid4(), "description": "No contact"}
]



  customer_info_records = []

  for i in range(NO_OF_CUSTOMER_INFO_ROWS):

    customer_info_record = {
      'customer_id': uuid.uuid4(),
      'first_name': fake.first_name(),
      'last_name': fake.last_name(),
      'email': f"{fake.first_name().lower()}.{fake.last_name().lower()}@" + random.choice(['email.com', 'inlook.com', 'mzn.com', 'aio.net', 'macrosoft.com' ]),
      'place_of_birth': fake.city(),
      'dob' : fake.date_of_birth(),
      'age': random.uniform(1, 100),
      'address': fake.address(),
      'city': fake.city(),
      'state': fake.state(),
      'zip': fake.zip(),
      'phone': fake.phone_number(),
      'credit_card': fake.credit_card_number(),
      'credit_card_provider': fake.credit_card_provider(),
      'nationality': fake.country(),
      'created_date': created_date,
      'last_updated_date': created_date + pd.Timedelta(days=random.randint(1, 40)),
      'customer_contact_preference_id': str(uuid.uuid4()),
      'customer_contact_preference_desc': random.choice(list(preferred_contact_method))
    }
    customer_info_records.append(customer_info_record)


  customer_info_df = pd.DataFrame(customer_info_records)


   # Write dataframe to JSON file
  with open(f'{DATASETS_LOCATION_PATH}/customer_info.json', 'wb') as customer_info_file:
      customer_info_df_to_json = customer_info_df.to_json(orient="records").encode('utf-8')
      customer_info_file.write(customer_info_df_to_json)
 


  # Print the customer information title in console
  print('----------')
  print('============================ CUSTOMER INFORMATION ============================')
  print(customer_info_df)
  


  # ========================================================================================================================================================================



  # ============================ FLIGHT SCHEDULES ============================

   # Create a Faker instance to generate fake data
  fake = Faker()


  flight_schedules = []
  # for i in range(NO_OF_FLIGHT_SCHEDULES):
  #   flight_id = uuid.uuid4()
  #   departure_city = fake.city()
  #   arrival_city = fake.city()
  #   departure_time = fake.time()
  #   arrival_time = fake.time()
  #   flight_date = fake.date_this_decade()











generate_travel_data()