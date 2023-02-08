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
  NO_OF_CUSTOMER_FEEDBACKS = 100
  NO_OF_TICKET_PRICES = 100
  NO_OF_FLIGHT_BOOKINGS = 100
  NO_OF_FLIGHT_DESTINATIONS = 100
  NO_OF_FLIGHT_TICKET_SALES = 100
  NO_OF_FLIGHT_PROMOS_AND_DEALS = 100
  NO_OF_SALES_AGENTS = 100



  # ========================================================================================================================================================================

  # ============================ CUSTOMER INFORMATION ============================

   # Create a Faker instance to generate fake data
  fake = Faker()


  created_date = fake.date_this_decade()
  
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
      'zip': fake.zipcode(),
      'phone_number': fake.phone_number(),
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
  # print(customer_info_df)


   # Write dataframe to JSON file
  with open(f'{DATASETS_LOCATION_PATH}/customer_info.json', 'w') as customer_info_file:
      customer_info_df_to_json = customer_info_df.to_json(orient="records", default_handler=str)
      customer_info_df_to_json = json.loads(customer_info_df_to_json)
      customer_info_file.write(json.dumps(customer_info_df_to_json, indent=4, sort_keys=True)) 

    

 


  # Print the customer information title in console
  print('----------')
  print('============================ CUSTOMER INFORMATION ============================')
  print(customer_info_df)
  


  # ========================================================================================================================================================================



  # ============================ FLIGHT SCHEDULES ============================

   # Create a Faker instance to generate fake data
  fake = Faker()


  flight_schedules = []
  for i in range(NO_OF_FLIGHT_SCHEDULES):
    flight_schedule = {
    'flight_id' : uuid.uuid4(),
    'departure_city' : fake.city(),
    'arrival_city' : fake.city(),
    'departure_time' : fake.time(),
    'arrival_time' : fake.time(),
    'flight_date' : fake.date_this_decade()

    }
    flight_schedules.append(flight_schedule)


    flight_schedules_df = pd.DataFrame(flight_schedules)
    # print(flight_schedules_df)


     # Write dataframe to JSON file
  with open(f'{DATASETS_LOCATION_PATH}/flight_schedules.json', 'w') as flight_schedules_file:
      flight_schedules_df_to_json = flight_schedules_df.to_json(orient="records", default_handler=str)
      flight_schedules_df_to_json = json.loads(flight_schedules_df_to_json)
      flight_schedules_file.write(json.dumps(flight_schedules_df_to_json, indent=4, sort_keys=True)) 

    

 


  # Print the customer information title in console
  print('----------')
  print('============================ FLIGHT SCHEDULES ============================')
  print(flight_schedules_df)
  


  # ========================================================================================================================================================================


   # ============================ TICKET PRICES ============================

   # Create a Faker instance to generate fake data
  fake = Faker()


  ticket_prices = []
  for i in range(NO_OF_TICKET_PRICES):
     ticket_price = {
        'flight_id' : random.choice(flight_schedules_df['flight_id']),
        'ticket_price': random.randint(50, 700),
        'ticket_price_date': fake.date_this_decade()

     }
     ticket_prices.append(ticket_price)

  ticket_prices_df = pd.DataFrame(ticket_prices)

  # Write dataframe to JSON file
  with open(f'{DATASETS_LOCATION_PATH}/ticket_prices.json', 'w') as ticket_prices_file:
      ticket_prices_df_to_json = ticket_prices_df.to_json(orient="records", default_handler=str)
      ticket_prices_df_to_json = json.loads(ticket_prices_df_to_json)
      ticket_prices_file.write(json.dumps(ticket_prices_df_to_json, indent=4, sort_keys=True)) 



  # Print the customer information title in console
  print('----------')
  print('============================ TICKET PRICES ============================')
  print(ticket_prices_df)
  



  # ============================ FLIGHT BOOKINGS ============================

   # Create a Faker instance to generate fake data
  fake = Faker()


  flight_bookings = []

  for i in range(NO_OF_FLIGHT_BOOKINGS):
     flight_booking = {
        'flight_booking_id': uuid.uuid4(),
        'customer_id' : random.choice(customer_info_df['customer_id']),
        'flight_id' : random.choice(flight_schedules_df['flight_id']),
        'ticket_price' : random.randint(0, 700),
        'booking_date': fake.date_this_decade()
     }
     flight_bookings.append(flight_booking)
  

  flight_bookings_df = pd.DataFrame(flight_bookings)

  # Write dataframe to JSON file
  with open(f'{DATASETS_LOCATION_PATH}/flight_bookings.json', 'w') as flight_bookings_file:
      flight_bookings_df_to_json = flight_bookings_df.to_json(orient="records", default_handler=str)
      flight_bookings_df_to_json = json.loads(flight_bookings_df_to_json)
      flight_bookings_file.write(json.dumps(flight_bookings_df_to_json, indent=4, sort_keys=True)) 



  # Print the customer information title in console
  print('----------')
  print('============================ FLIGHT BOOKINGS ============================')
  print(flight_bookings_df)
  




  # ============================ CUSTOMER FEEDBACK ============================

   # Create a Faker instance to generate fake data
  fake = Faker()


  customer_feedbacks = []
  for i in range(NO_OF_CUSTOMER_FEEDBACKS):
     customer_feedback = {
     'feedback_id' : uuid.uuid4(),
     'customer_id': random.choice(customer_info_df['customer_id']),
     'flight_booking_id': random.choice(flight_bookings_df['flight_booking_id']),
     'feedback_text': fake.text(),
     'feedback_date': fake.date_this_decade()
     }

     customer_feedbacks.append(customer_feedback)


  
  customer_feedbacks_df = pd.DataFrame(customer_feedbacks)

  # Write dataframe to JSON file
  with open(f'{DATASETS_LOCATION_PATH}/customer_feedbacks.json', 'w') as customer_feedbacks_file:
      customer_feedbacks_df_to_json = customer_feedbacks_df.to_json(orient="records", default_handler=str)
      customer_feedbacks_df_to_json = json.loads(customer_feedbacks_df_to_json)
      customer_feedbacks_file.write(json.dumps(customer_feedbacks_df_to_json, indent=4, sort_keys=True)) 



  # Print the customer information title in console
  print('----------')
  print('============================ CUSTOMER FEEDBACKS ============================')
  print(customer_feedbacks_df)
  
    

  # ============================ SALES AGENTS ============================


  fake = Faker()

  

  # List of sales agents
  agents = []

  # Generate the sales agents
  for i in range(NO_OF_SALES_AGENTS):
    agent_id = str(uuid.uuid4())
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = f"{first_name.lower()}.{last_name.lower()}@" + random.choice(['gmail.com', 'outlook.com', 'msn.com', 'aoi.net', 'microsoft.com' ])
    phone = fake.phone_number()
    location = fake.city()
    service_speciality = random.choice(['Air', 'Land', 'Sea'])
    years_experience = random.randint(1, 10)
    commission = random.uniform(0.1, 0.3)
    nationality = fake.country()
    seniority_levels = ['Junior', 'Mid-Level', 'Senior']
    seniority_level = random.choice(seniority_levels)
    agents.append({
        'id': agent_id,
        'first_name': first_name,
        'last_name': last_name,
        'email': email,
        'phone': phone,
        'location': location,
        'service_speciality': service_speciality,
        'years_experience': years_experience,
        'commission': commission,
        'nationality': nationality,
        'seniority_level': seniority_level
    })

  # Create a Pandas DataFrame from the list of options
  sales_agents_df = pd.DataFrame(agents)


  # # Write dataframe to JSON file 
  with open(f'{DATASETS_LOCATION_PATH}/sales_agents.json', 'w') as sales_agents_file:
    sales_agents_file_df_to_json = sales_agents_df.to_json(orient="records")
    sales_agents_file.write(json.dumps(json.loads(sales_agents_file_df_to_json), indent=4, sort_keys=True)) 


  # Print the data frame
  print('----------')
  print('============================ SALES AGENTS ============================')
  print(sales_agents_df)





  # ============================ FLIGHT DESTINATION ============================

   # Create a Faker instance to generate fake data
  fake = Faker()

  flight_destinations = []

  for i in range(NO_OF_FLIGHT_DESTINATIONS):
     flight_destination = {
        'flight_id' : random.choice(flight_bookings_df['flight_id']),
        'departure_city' : fake.city(),
        'arrival_city': fake.city() 

     }

     flight_destinations.append(flight_destination)

  flight_destinations_df = pd.DataFrame(flight_destinations)

  # Write dataframe to JSON file
  with open(f'{DATASETS_LOCATION_PATH}/flight_destinations.json', 'w') as flight_destinations_file:
      flight_destinations_df_to_json = flight_destinations_df.to_json(orient="records", default_handler=str)
      flight_destinations_df_to_json = json.loads(flight_destinations_df_to_json)
      flight_destinations_file.write(json.dumps(flight_destinations_df_to_json, indent=4, sort_keys=True)) 



  # Print the customer information title in console
  print('----------')
  print('============================ FLIGHT DESTINATIONS ============================')
  print(flight_destinations_df)
  
    




  # ============================ FLIGHT TICKET SALES ============================

   # Create a Faker instance to generate fake data
  fake = Faker()

  flight_ticket_sales = []

  for i in range(NO_OF_FLIGHT_TICKET_SALES):
     flight_ticket_sale = {
        'flight_id': random.choice(flight_bookings_df['flight_id']),
        'customer_id': random.choice(customer_info_df['customer_id']),
        'customer_first_name': random.choice(customer_info_df['first_name']),
        'customer_last_name': random.choice(customer_info_df['last_name']),
        'agent_id': random.choice(sales_agents_df['id']),
        'agent_first_name': random.choice(sales_agents_df['first_name']),
        'agent_last_name': random.choice(sales_agents_df['last_name']),
        'ticket_sales': random.randint(100, 500),
        'ticket_sales_date': fake.date_this_decade()
     }

     flight_ticket_sales.append(flight_ticket_sale)
  
  flight_ticket_sales_df = pd.DataFrame(flight_ticket_sales)

  # Write dataframe to JSON file
  with open(f'{DATASETS_LOCATION_PATH}/flight_ticket_sales.json', 'w') as flight_ticket_sales_file:
      flight_ticket_sales_df_to_json = flight_ticket_sales_df.to_json(orient="records", default_handler=str)
      flight_ticket_sales_df_to_json = json.loads(flight_ticket_sales_df_to_json)
      flight_ticket_sales_file.write(json.dumps(flight_ticket_sales_df_to_json, indent=4, sort_keys=True)) 



  # Print the customer information title in console
  print('----------')
  print('============================ FLIGHT TICKET SALES ============================')
  print(flight_ticket_sales_df)
  

     




  # ============================ FLIGHT PROMOTIONS & DEALS ============================

   # Create a Faker instance to generate fake data
  fake = Faker()

  flight_promotion_deals = []

  for i in range(NO_OF_FLIGHT_PROMOS_AND_DEALS):
     flight_promotion_deal = {
        'promotion_id': uuid.uuid4(),
        'promotion_name': fake.sentence(), 
        'flight_booking_id': random.choice(flight_bookings_df['flight_booking_id']),
        'applied_discount': random.randint(10, 350)

     }
     flight_promotion_deals.append(flight_promotion_deal)

  
  flight_promotion_deals_df = pd.DataFrame(flight_promotion_deals)

    # Write dataframe to JSON file
  with open(f'{DATASETS_LOCATION_PATH}/flight_promotion_deals.json', 'w') as flight_promotion_deals_file:
      flight_promotion_deals_df_to_json = flight_promotion_deals_df.to_json(orient="records", default_handler=str)
      flight_promotion_deals_df_to_json = json.loads(flight_promotion_deals_df_to_json)
      flight_promotion_deals_file.write(json.dumps(flight_promotion_deals_df_to_json, indent=4, sort_keys=True)) 



  # Print the customer information title in console
  print('----------')
  print('============================ FLIGHT PROMOTIONS & DEALS ============================')
  print(flight_promotion_deals_df)
  

     
     








generate_travel_data()