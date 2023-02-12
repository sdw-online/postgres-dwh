
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

1. stg_accommodation_bookings_tbl

- Database connection check: Check if the connection to the database is successful
- Schema existence check : Check if the schema exists in the database
- Table existence check: Check if the table exists in the database
- Columns existence checks: Check if the columns are present in the database
- Data type checks: Check if the actual data types of each column matched the expected data types
- Empty values checks: Check if there are any empty values present in the table
- Not NULL checks: Check if there are any NULLs present in the table
- Table row + column count: Calculate the total number of rows and columns in each table
- Total records uploaded successfully/unsuccessfully: Calculate the total number of records uploaded to the table successfully
- ID formatting checks: Check if the ID columns are 36 characters in length
- Date formatting checks: Check if the date formats match the `yy-mm-dd` format
- Date range checks: Check if the date ranges are between 2012 and 2022
- Date-time checks: Check if the date times are accurately represented
- Confirmation code formatting check: Check the confirmation code for each booking is 10 characters in length
- Payment method domain check: Check the payment method only contain the "Debit card", "Credit card", "Paypal" and "Bank transfer" options
- Status domain check: Check the status column only contains “confirmed” and “cancelled” options

2. stg_customer_feedbacks_tbl


- Database connection check: Check if the connection to the database is successful
- Schema existence check : Check if the schema exists in the database
- Table existence check: Check if the table exists in the database
- Columns existence checks: Check if the columns are present in the database
- Data type checks: Check if the actual data types of each column matched the expected data types
- Empty values checks: Check if there are any empty values present in the table
- Not NULL checks: Check if there are any NULLs present in the table
- Table row + column count: Calculate the total number of rows and columns in each table
- Total records uploaded successfully/unsuccessfully: Calculate the total number of records uploaded to the table successfully
- ID formatting checks: Check if the ID columns are 36 characters in length
- Date formatting checks: Check if the date formats match the `yy-mm-dd` format
- Date range checks: Check if the date ranges are between 2012 and 2022

3. stg_customer_info_tbl

- Database connection check: Check if the connection to the database is successful
- Schema existence check : Check if the schema exists in the database
- Table existence check: Check if the table exists in the database
- Columns existence checks: Check if the columns are present in the database
- Data type checks: Check if the actual data types of each column matched the expected data types
- Empty values checks: Check if there are any empty values present in the table
- Not NULL checks: Check if there are any NULLs present in the table
- Table row + column count: Calculate the total number of rows and columns in each table
- Total records uploaded successfully/unsuccessfully: Calculate the total number of records uploaded to the table successfully
- ID formatting checks: Check if the ID columns are 36 characters in length
- Date formatting checks: Check if the date formats match the `yy-mm-dd` format
- Date range checks: Check if the date ranges are between 2012 and 2022
- Date-time checks: Check if the date times are accurately represented
- Age range check: Check the age values are between 0 and 110

4. stg_flight_bookings_tbl
- Database connection check: Check if the connection to the database is successful
- Schema existence check : Check if the schema exists in the database
- Table existence check: Check if the table exists in the database
- Columns existence checks: Check if the columns are present in the database
- Data type checks: Check if the actual data types of each column matched the expected data types
- Empty values checks: Check if there are any empty values present in the table
- Not NULL checks: Check if there are any NULLs present in the table
- Table row + column count: Calculate the total number of rows and columns in each table
- Total records uploaded successfully/unsuccessfully: Calculate the total number of records uploaded to the table successfully
- ID formatting checks: Check if the ID columns are 36 characters in length
- Date formatting checks: Check if the date formats match the `yy-mm-dd` format
- Date range checks: Check if the date ranges are between 2012 and 2022
- Date-time checks: Check if the date times are accurately represented
- Ticket price range check: Check if the ticket prices are between 1 and 700

5. stg_flight_destinations_tbl

- Database connection check: Check if the connection to the database is successful
- Schema existence check : Check if the schema exists in the database
- Table existence check: Check if the table exists in the database
- Columns existence checks: Check if the columns are present in the database
- Data type checks: Check if the actual data types of each column matched the expected data types
- Empty values checks: Check if there are any empty values present in the table
- Not NULL checks: Check if there are any NULLs present in the table
- Table row + column count: Calculate the total number of rows and columns in each table
- Total records uploaded successfully/unsuccessfully: Calculate the total number of records uploaded to the table successfully
- ID formatting checks: Check if the ID columns are 36 characters in length
- Date formatting checks: Check if the date formats match the `yy-mm-dd` format
- Date range checks: Check if the date ranges are between 2012 and 2022
- Date-time checks: Check if the date times are accurately represented

6. stg_flight_promotion_deals_tbl

- Database connection check: Check if the connection to the database is successful
- Schema existence check : Check if the schema exists in the database
- Table existence check: Check if the table exists in the database
- Columns existence checks: Check if the columns are present in the database
- Data type checks: Check if the actual data types of each column matched the expected data types
- Empty values checks: Check if there are any empty values present in the table
- Not NULL checks: Check if there are any NULLs present in the table
- Table row + column count: Calculate the total number of rows and columns in each table
- Total records uploaded successfully/unsuccessfully: Calculate the total number of records uploaded to the table successfully
- ID formatting checks: Check if the ID columns are 36 characters in length

7. stg_flight_schedules_tbl

- Database connection check: Check if the connection to the database is successful
- Schema existence check : Check if the schema exists in the database
- Table existence check: Check if the table exists in the database
- Columns existence checks: Check if the columns are present in the database
- Data type checks: Check if the actual data types of each column matched the expected data types
- Empty values checks: Check if there are any empty values present in the table
- Not NULL checks: Check if there are any NULLs present in the table
- Table row + column count: Calculate the total number of rows and columns in each table
- Total records uploaded successfully/unsuccessfully: Calculate the total number of records uploaded to the table successfully
- ID formatting checks: Check if the ID columns are 36 characters in length
- Date formatting checks: Check if the date formats match the `yy-mm-dd` format
- Date range checks: Check if the date ranges are between 2012 and 2022
- Date-time checks: Check if the date times are accurately represented

8. stg_flight_ticket_sales_tbl

- Database connection check: Check if the connection to the database is successful
- Schema existence check : Check if the schema exists in the database
- Table existence check: Check if the table exists in the database
- Columns existence checks: Check if the columns are present in the database
- Data type checks: Check if the actual data types of each column matched the expected data types
- Empty values checks: Check if there are any empty values present in the table
- Not NULL checks: Check if there are any NULLs present in the table
- Table row + column count: Calculate the total number of rows and columns in each table
- Total records uploaded successfully/unsuccessfully: Calculate the total number of records uploaded to the table successfully
- ID formatting checks: Check if the ID columns are 36 characters in length
- Date formatting checks: Check if the date formats match the `yy-mm-dd` format
- Date range checks: Check if the date ranges are between 2012 and 2022
- Date-time checks: Check if the date times are accurately represented
- Ticket price range check: Check the ticket prices are between 1 and 700

9. stg_sales_agents_tbl

- Database connection check: Check if the connection to the database is successful
- Schema existence check : Check if the schema exists in the database
- Table existence check: Check if the table exists in the database
- Columns existence checks: Check if the columns are present in the database
- Data type checks: Check if the actual data types of each column matched the expected data types
- Empty values checks: Check if there are any empty values present in the table
- Not NULL checks: Check if there are any NULLs present in the table
- Table row + column count: Calculate the total number of rows and columns in each table
- Total records uploaded successfully/unsuccessfully: Calculate the total number of records uploaded to the table successfully
- ID formatting checks: Check if the ID columns are 36 characters in length
- Date formatting checks: Check if the date formats match the `yy-mm-dd` format
- Date range checks: Check if the date ranges are between 2012 and 2022
- Date-time checks: Check if the date times are accurately represented
- Ticket price range check: Check if the ticket prices are between 1 and 700
- Seniority domain check: Check the seniority column only contains the “Junior”, “Mid-level” and “Senior” values

10. stg_ticket_prices_tbl

- Database connection check: Check if the connection to the database is successful
- Schema existence check : Check if the schema exists in the database
- Table existence check: Check if the table exists in the database
- Columns existence checks: Check if the columns are present in the database
- Data type checks: Check if the actual data types of each column matched the expected data types
- Empty values checks: Check if there are any empty values present in the table
- Not NULL checks: Check if there are any NULLs present in the table
- Table row + column count: Calculate the total number of rows and columns in each table
- Total records uploaded successfully/unsuccessfully: Calculate the total number of records uploaded to the table successfully
- ID formatting checks: Check if the ID columns are 36 characters in length
- Date formatting checks: Check if the date formats match the `yy-mm-dd` format
- Date range checks: Check if the date ranges are between 2012 and 2022
- Date-time checks: Check if the date times are accurately represented
- Ticket price range check: Check if the ticket prices are between 1 and 700
