# ERD Checklist


Specify the entity relationships via a diagram to have a graphical perspective to the semantic layer's database structure in Postgres. 


## Tables

* xxxxxxxxx
* xxxxxxxxx
* xxxxxxxxx
* xxxxxxxxx
* xxxxxxxxx
* xxxxxxxxx

***
---

## Fact tables 

### 1. fact_sales_tbl

| index | connecting_table | key_type_required |  cardinality    |  description     |  example     |
| ----- | --------------   | ---------------  | ---------------  |  --------------- |  --------------- |
| 1     | dim_sales_employees_tbl   | FK   | zero-to-many (0:M)   | One sales transaction can only be processed by one sales agent at a time, but a sales agent can close multiple sales transactions at any given time  |  **1)** A sales agent managed to broker 19 travel bookings, 14 hotel/motel bookings and 5 tourist souvenirs over the past week, and **2)** an average of 205+ airplane tickets were purchased online daily during 2021 without sales agents required |
| 2     | fact_travel_bookings_tbl   | FK   | one-to-many (1:M)   | One sale transaction can only be linked to a single travel booking, but one travel booking can be linked to multiple sales transactions    |  A travel booking can contain one accommodation booking, one tourist souvenir, additional services etc    |
| 3     | fact_accommodations_tbl   | FK   | one-to-many (1:M)   | One sale transaction can only be linked to a single accommodation booking, but one accommodation booking can be linked to more than one sales transactions   | One AirBnb room booking can include wifi, breakfast-in-bed, room service etc   |
| 4     | dim_customers_tbl   | FK   | one-to-many (1:M)   | One sale can only be made by one customer at a time, but one customer can be linked to multiple sales   | A customer can book a plane ticket (return) to Edinburgh, book a hotel for a week, and decide to extend their stay (which requires their original plane ticket & hotel reservations needing revision)   | 

***



### 2. fact_accommodations_tbl

| index | connecting_table | key_type_required |  cardinality    |  description     |  example         |  join_table (Y/N) |
| ----- | --------------   | ---------------  | ---------------  | ---------------  | ---------------  | ---------------  |
| 1     | fact_sales_tbl    | PK   | many-to-one (M:1)   | One sale transaction can only be linked to one accommodation booking, but one accommodation booking can be linked to multiple sales transactions   | One hotel room booking can include additional services like breakfast, gym + swimming pool access, one-hour spa package etc      | Y   |
| 2     | dim_customers_tbl   | FK   | one-to-many (1:M)   | One accommodation booking can only be made by a customer, but one customer can make multiple accommodation bookings over a period of time    | A consultant makes regular hotel bookings on a quarterly basis in New York for business meetings at headquarters   | Y   |

***
---
***

## Dimension tables 




### 1. dim_customers_tbl

| index | connecting_table | key_type_required |  cardinality    |  description     |  example         |  join_table (Y/N) |
| ----- | --------------   | ---------------  | ---------------  | ---------------  | ---------------  | ---------------  |
| 1     | fact_sales_tbl   | PK   | many-to-one (M:1)   | A customer can place an order for several sale items at any given time, but one sale item ordered for can only be attributed to a single customer  | A tourist can purchase multiple historical souvenirs, food and drinks during a tour  | Y   |
| 2     | fact_travel_bookings_tbl   | PK   | many-to-one (M:1)   | One customer can purchase multiple travel tickets over a period of time, but one travel ticket can only be attributed to a single customer   | An office clerk regularly tops up their Oyster card to travel from Greenwich to Westminster via TFL trains for work    | Y   |
| 3     | fact_accommodations_tbl   | PK   | many-to-one (M:1)   | One customer can make multiple accommodation bookings over a period of time, but one accommodation booking can only be attributed to the customer who made the payment and reservation   | An entrepreneur books the same Airbnb room in Hither Green every 2-3 months for his family to engage in mini-retreats    | Y   |

***


### 2. dim_sales_employees_tbl

| index | connecting_table | key_type_required |  cardinality    |  description     |  example         |  join_table (Y/N) |
| ----- | --------------   | ---------------  | ---------------  | ---------------  | ---------------  | ---------------  |
| 1     | fact_sales_tbl   | PK   | many-to-zero (M:0)   | One sales agent can broker multiple sales at a period of time, but one sale transaction can only be attributed to either 0 or 1 sale agent   | Over 400+ train travel-cards were purchased via an AI chatbot assistant instead of actual sales agent during June 2022   | Y |
| 2     | fact_travel_bookings_tbl   | PK   | many-to-zero (M:0)   | One sale agent can sell multiple travel tickets over a period of time, but one travel booking can only be attributed to either 0 or 1 sale agent     | A sales agent that specializes in bus sales managed to sell 175+ bus passes in September 2018, but didn't sell any in September 2022 even though more passengers purchased in 2022 than 2018 (because passengers find it more convenient purchasing via one-click apps) | Y  |


***


***

### 3. dim_prices_tbl

| index | connecting_table | key_type_required |  cardinality    |  description     |  example         |  join_table (Y/N) |
| ----- | --------------   | ---------------  | ---------------  | ---------------  | ---------------  | ---------------  |
| 1     | raw_countries_tbl   | FK   | one-to-one (1:1)   | One sale item price can only relate to one country record, and each country record can only be linked to one sale item price in the listing    | A flight ticket from London-UK to Houston-Texas would be displayed to UK customers in GBP (£) and USA customers in USD ($)   | Y   |

***

### 4. xxxxxxxxx

| index | connecting_table | key_type_required |  cardinality    |  description     |  example         |  join_table (Y/N) |
| ----- | --------------   | ---------------  | ---------------  | ---------------  | ---------------  | ---------------  |
| 1     | raw_countries_tbl   | FK   | many-to-one (M:1)   | One country code can be linked to multiple foreign exchange rates over a period of time , but one fx rate record can only be linked to one country code | The exchange rate for the British pound sterling (GBP) has fluctuated over time due to unpredictable economic indicators like inflation, GDP (i.e. GBP has multiple rates over a 10 year period)     | Y   |

***

### 5. xxxxxxxxx

| index | connecting_table | key_type_required |  cardinality    |  description     |  example         |  join_table (Y/N) |
| ----- | --------------   | ---------------  | ---------------  | ---------------  | ---------------  | ---------------  |
| 1     | raw_countries_tbl   | FK   | many-to-one (M:1)   | One country code can be linked to multiple foreign exchange rates over a period of time , but one fx rate record can only be linked to one country code | The exchange rate for the British pound sterling (GBP) has fluctuated over time due to unpredictable economic indicators like inflation, GDP (i.e. GBP has multiple rates over a 10 year period)     | Y   |


***

### 6. xxxxxxxxx

| index | connecting_table | key_type_required |  cardinality    |  description     |  example         |  join_table (Y/N) |
| ----- | --------------   | ---------------  | ---------------  | ---------------  | ---------------  | ---------------  |
| 1     | raw_countries_tbl   | FK   | many-to-one (M:1)   | One country code can be linked to multiple foreign exchange rates over a period of time , but one fx rate record can only be linked to one country code | The exchange rate for the British pound sterling (GBP) has fluctuated over time due to unpredictable economic indicators like inflation, GDP (i.e. GBP has multiple rates over a 10 year period)     | Y   |

***

### 7. xxxxxxxxx

| index | connecting_table | key_type_required |  cardinality    |  description     |  example         |  join_table (Y/N) |
| ----- | --------------   | ---------------  | ---------------  | ---------------  | ---------------  | ---------------  |
| 1     | raw_countries_tbl   | FK   | many-to-one (M:1)   | One country code can be linked to multiple foreign exchange rates over a period of time , but one fx rate record can only be linked to one country code | The exchange rate for the British pound sterling (GBP) has fluctuated over time due to unpredictable economic indicators like inflation, GDP (i.e. GBP has multiple rates over a 10 year period)     | Y   |


***

### 8. xxxxxxxxx

| index | connecting_table | key_type_required |  cardinality    |  description     |  example         |  join_table (Y/N) |
| ----- | --------------   | ---------------  | ---------------  | ---------------  | ---------------  | ---------------  |
| 1     | raw_countries_tbl   | FK   | many-to-one (M:1)   | One country code can be linked to multiple foreign exchange rates over a period of time , but one fx rate record can only be linked to one country code | The exchange rate for the British pound sterling (GBP) has fluctuated over time due to unpredictable economic indicators like inflation, GDP (i.e. GBP has multiple rates over a 10 year period)     | Y   |