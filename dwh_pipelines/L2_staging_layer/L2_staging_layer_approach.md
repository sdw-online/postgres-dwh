
# Approach: Staging Layer


## Acceptance criteria 

- The raw data must load into the staging tables with no issues 
- The transformation strategy must be established and broken down into individual steps
- The data quality rules and constraints must be defined
- The QA tests must be defined and executed
- The transformation strategy must be executed (converting transformation steps into coding logic)
- The transformed data must be loaded into 2nd level staging tables
- A program must log the events for the staging processes to a file and console
- Data profiling metrics must be logged to a file and console to understand the staging data’s properties and structure

## Completion 

Once the acceptance criteria is 100% covered and satisfied, the staging layer’s tasks are officially completed.




# Transformation strategy

Here is the strategy for cleaning each of the staging tables in preparation for the semantic layer:

1. stg_accommodation_bookings_tbl

- Convert date fields (`booking_date`, `check_in_date`, `check_out_date`) from integer to date type (with ‘yyyy-mm-dd’)
- Round `total_price` column to 2dp
- Rename `num_adults` to `no_of_adults`
- Rename `num_children` to `no_of_children`

2. stg_customer_feedbacks_tbl
- Convert `feedback_date` field from integer to date type (with ‘yyyy-mm-dd’)

3. stg_customer_info_tbl
- Convert `age` field to integer
- Convert date fields (`created_date`, `dob`) from integer to date type (with ‘yyyy-mm-dd’)
- Concatenate `first_name` and `last_name` fields to create `full_name` column
- Explode nested dictionary in `customer_contact_preference_desc` into separate columns
- Replace original `customer_contact_preference_id` field with the nested one

4. stg_flight_bookings_tbl

- Round `ticket_price` column to 2dp
- Convert `booking_date` from integer to date type (with ‘yyyy-mm-dd’)

5. stg_flight_destinations_tbl

(None required)

6. stg_flight_promotion_deals_tbl

(None required)

7. stg_flight_schedules_tbl

- Convert `flight_date` field from integer to date type (with ‘yyyy-mm-dd’)
- Convert time columns (`departure_time`, `arrival_time`) from `h:m:s` to `h:m` format
- Add `arrival_date` column (conditional column: if the hour for new arrival time is greater than 24,make the arrival date one day later than departure date and set arrival time to… )

8. stg_flight_ticket_sales_tbl

- Round price columns (`discount`, `ticket_sales`) to 2dp
- Convert `ticket_sales_date` from integer to date type (with ‘yyyy-mm-dd’)

9. stg_sales_agents_tbl

- Concatenate agent `first_name` and `last_name` fields to create `full_name` column

10. stg_ticket_prices_tbl

- Round `ticket_price` column to 2dp
- Convert `ticket_price_date` from integer to date type (with ‘yyyy-mm-dd’)



# Data Validation & Quality Checks


The next step is data validation and quality checks, which are a series of tests to verify how integral the processed data is up to this point. These checks include:

- Database connection check
- Schema existence check
- Table existence check
- Columns existence checks
- Data type checks
- Empty values checks
- Not NULL checks
- Table row + column count
- Total records uploaded successfully/unsuccessfully
- Email formatting check
- Date formatting check
- Age range check
- Customer range check (adults)
- Customer range check (children)

## 1. stg_accommodation_bookings_tbl


| index | check_type             | dq_rule |
| ----- | -------------- | --------------- | 
| 1     | Database Connection | Program must verify if the connection to the database is successful/unsuccessful |
| 2     | Schema Existence | Program must verify if the active schema exists in the database |
| 3     | Table Existence | Program must verify if the active table exists in the database |
| 4     | Columns Existence | Program must verify if the columns exists in the active table |
| 5     | Data Type | The **actual** data types of each table column must match the **expected** data type |
| 6     | Empty Values | The table must not contain any empty values |
| 7     | Null Values | The table must not contain any `NULL` values |
| 8     | Row count | The total rows in the staging table must match the total row count in the raw table version (as no strict business rules are applied at this layer)  |
| 9     | Date format | Date columns must be in "yyyy-mm-dd" format |
| 10    | Date range | Date columns must be between `01-01-2012` and `31-12-2022` |
| 11    | Date domain | The value in the `check_in_date` field must be before the value in the `check_out_date` field |
| 12    | Confirmation code | Each confirmation code value must be 36 characters in length |
| 13    | Payment method | Payment method column should only contain "Debit card", "Credit card", "PayPal" and "Bank transfer" values |
| 14    | Status | Status column should only contain "Pending", "Confirmed" and "Cancelled" values |
| 15    | ID format | ID columns should be strings of 36 characters in length |



## 2. stg_customer_feedbacks_tbl



| index | check_type             | dq_rule |
| ----- | -------------- | --------------- | 
| 1     | Database Connection | Program must verify if the connection to the database is successful/unsuccessful |
| 2     | Schema Existence | Program must verify if the active schema exists in the database |
| 3     | Table Existence | Program must verify if the active table exists in the database |
| 4     | Columns Existence | Program must verify if the columns exists in the active table |
| 5     | Data Type | The **actual** data types of each table column must match the **expected** data type |
| 6     | Empty Values | The table must not contain any empty values |
| 7     | Null Values | The table must not contain any `NULL` values |
| 8     | Row count | The total rows in the staging table must match the total row count in the raw table version (as no strict business rules are applied at this layer)  |
| 9     | Date format | Date columns must be in "yyyy-mm-dd" format |
| 10    | ID format | ID columns should be strings of 36 characters in length |
| 11    | Date range | Date columns must be between `01-01-2012` and `31-12-2022` |






## 3. stg_customer_info_tbl


| index | check_type             | dq_rule |
| ----- | -------------- | --------------- | 
| 1     | Database Connection | Program must verify if the connection to the database is successful/unsuccessful |
| 2     | Schema Existence | Program must verify if the active schema exists in the database |
| 3     | Table Existence | Program must verify if the active table exists in the database |
| 4     | Columns Existence | Program must verify if the columns exists in the active table |
| 5     | Data Type | The **actual** data types of each table column must match the **expected** data type |
| 6     | Empty Values | The table must not contain any empty values |
| 7     | Null Values | The table must not contain any `NULL` values |
| 8     | Row count | The total rows in the staging table must match the total row count in the raw table version (as no strict business rules are applied at this layer)  |
| 9     | Date format | Date columns must be in "yyyy-mm-dd" format |
| 10    | ID format | ID columns should be strings of 36 characters in length |
| 11    | Date range | Date columns must be between `01-01-2012` and `31-12-2022` |
| 12    | Age range | Age should be a value between 0 and 110 |






## 4. stg_flight_bookings_tbl


| index | check_type             | dq_rule |
| ----- | -------------- | --------------- | 
| 1     | Database Connection | Program must verify if the connection to the database is successful/unsuccessful |
| 2     | Schema Existence | Program must verify if the active schema exists in the database |
| 3     | Table Existence | Program must verify if the active table exists in the database |
| 4     | Columns Existence | Program must verify if the columns exists in the active table |
| 5     | Data Type | The **actual** data types of each table column must match the **expected** data type |
| 6     | Empty Values | The table must not contain any empty values |
| 7     | Null Values | The table must not contain any `NULL` values |
| 8     | Row count | The total rows in the staging table must match the total row count in the raw table version (as no strict business rules are applied at this layer)  |
| 9     | Date format | Date columns must be in "yyyy-mm-dd" format |
| 10    | ID format | ID columns should be strings of 36 characters in length |
| 11    | Date range | Date columns must be between `01-01-2012` and `31-12-2022` |
| 12    | Ticket price | Price column should be a positive value i.e. greater than 0 |




## 5. stg_flight_destinations_tbl




| index | check_type             | dq_rule |
| ----- | -------------- | --------------- | 
| 1     | Database Connection | Program must verify if the connection to the database is successful/unsuccessful |
| 2     | Schema Existence | Program must verify if the active schema exists in the database |
| 3     | Table Existence | Program must verify if the active table exists in the database |
| 4     | Columns Existence | Program must verify if the columns exists in the active table |
| 5     | Data Type | The **actual** data types of each table column must match the **expected** data type |
| 6     | Empty Values | The table must not contain any empty values |
| 7     | Null Values | The table must not contain any `NULL` values |
| 8     | Row count | The total rows in the staging table must match the total row count in the raw table version (as no strict business rules are applied at this layer)  |
| 9     | Date format | Date columns must be in "yyyy-mm-dd" format |
| 10    | ID format | ID columns should be strings of 36 characters in length |
| 11    | Date range | Date columns must be between `01-01-2012` and `31-12-2022` |



## 6. stg_flight_promotion_deals_tbl



| index | check_type             | dq_rule |
| ----- | -------------- | --------------- | 
| 1     | Database Connection | Program must verify if the connection to the database is successful/unsuccessful |
| 2     | Schema Existence | Program must verify if the active schema exists in the database |
| 3     | Table Existence | Program must verify if the active table exists in the database |
| 4     | Columns Existence | Program must verify if the columns exists in the active table |
| 5     | Data Type | The **actual** data types of each table column must match the **expected** data type |
| 6     | Empty Values | The table must not contain any empty values |
| 7     | Null Values | The table must not contain any `NULL` values |
| 8     | ID format | ID columns should be strings of 36 characters in length |




## 7. stg_flight_schedules_tbl



| index | check_type             | dq_rule |
| ----- | -------------- | --------------- | 
| 1     | Database Connection | Program must verify if the connection to the database is successful/unsuccessful |
| 2     | Schema Existence | Program must verify if the active schema exists in the database |
| 3     | Table Existence | Program must verify if the active table exists in the database |
| 4     | Columns Existence | Program must verify if the columns exists in the active table |
| 5     | Data Type | The **actual** data types of each table column must match the **expected** data type |
| 6     | Empty Values | The table must not contain any empty values |
| 7     | Null Values | The table must not contain any `NULL` values |
| 8     | Row count | The total rows in the staging table must match the total row count in the raw table version (as no strict business rules are applied at this layer)  |
| 9     | Date format | Date columns must be in "yyyy-mm-dd" format |
| 10    | ID format | ID columns should be strings of 36 characters in length |
| 11    | Date range | Date columns must be between `01-01-2012` and `31-12-2022` |




## 8. stg_flight_ticket_sales_tbl



| index | check_type             | dq_rule |
| ----- | -------------- | --------------- | 
| 1     | Database Connection | Program must verify if the connection to the database is successful/unsuccessful |
| 2     | Schema Existence | Program must verify if the active schema exists in the database |
| 3     | Table Existence | Program must verify if the active table exists in the database |
| 4     | Columns Existence | Program must verify if the columns exists in the active table |
| 5     | Data Type | The **actual** data types of each table column must match the **expected** data type |
| 6     | Empty Values | The table must not contain any empty values |
| 7     | Null Values | The table must not contain any `NULL` values |
| 8     | Row count | The total rows in the staging table must match the total row count in the raw table version (as no strict business rules are applied at this layer)  |
| 9     | Date format | Date columns must be in "yyyy-mm-dd" format |
| 10    | ID format | ID columns should be strings of 36 characters in length |
| 11    | Date range | Date columns must be between `01-01-2012` and `31-12-2022` |
| 12    | Ticket price | Price column should be a positive value i.e. greater than 0 |




## 9. stg_sales_agents_tbl



| index | check_type             | dq_rule |
| ----- | -------------- | --------------- | 
| 1     | Database Connection | Program must verify if the connection to the database is successful/unsuccessful |
| 2     | Schema Existence | Program must verify if the active schema exists in the database |
| 3     | Table Existence | Program must verify if the active table exists in the database |
| 4     | Columns Existence | Program must verify if the columns exists in the active table |
| 5     | Data Type | The **actual** data types of each table column must match the **expected** data type |
| 6     | Empty Values | The table must not contain any empty values |
| 7     | Null Values | The table must not contain any `NULL` values |
| 8     | Row count | The total rows in the staging table must match the total row count in the raw table version (as no strict business rules are applied at this layer)  |
| 9     | Date format | Date columns must be in "yyyy-mm-dd" format |
| 10    | Date range | Date columns must be between `01-01-2012` and `31-12-2022` |
| 11    | Ticket price | Price column should be a positive value i.e. greater than 0 |
| 12    | ID format | ID columns should be strings of 36 characters in length |
| 14    | Seniority | Seniority column should only contain “Junior”, “Mid-level” and “Senior” values |





## 10. stg_ticket_prices_tbl


| index | check_type             | dq_rule |
| ----- | -------------- | --------------- | 
| 1     | Database Connection | Program must verify if the connection to the database is successful/unsuccessful |
| 2     | Schema Existence | Program must verify if the active schema exists in the database |
| 3     | Table Existence | Program must verify if the active table exists in the database |
| 4     | Columns Existence | Program must verify if the columns exists in the active table |
| 5     | Data Type | The **actual** data types of each table column must match the **expected** data type |
| 6     | Empty Values | The table must not contain any empty values |
| 7     | Null Values | The table must not contain any `NULL` values |
| 8     | Row count | The total rows in the staging table must match the total row count in the raw table version (as no strict business rules are applied at this layer)  |
| 9     | Date format | Date columns must be in "yyyy-mm-dd" format |
| 10    | ID format | ID columns should be strings of 36 characters in length |
| 11    | Date range | Date columns must be between `01-01-2012` and `31-12-2022` |
| 12    | Ticket price | Price column should be a positive value i.e. greater than 0 |
