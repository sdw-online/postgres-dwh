# Approach: Raw Layer


## Acceptance criteria 

- The raw layer must extract data from various source systems with no issues
- A program must log the events for the extraction process (and other processes supporting extraction) to a file and console
- Sensitive fields and records must be highlighted to determine the appropriate treatment required
- Data profiling metrics must be logged to a file and console to understand the raw data’s properties and structure


### Completion 

Once the acceptance criteria is 100% covered and satisfied, the raw layer’s tasks are officially completed.


*** 



## Micro tasks

### Extracting source data to raw tables

- Load source data for “Customer information” into raw table `raw_customer_data`
- Load source data for “Flight schedules” into raw table `raw_flight_schedules_data`
- Load source data for “Customer feedback” into raw table `raw_customer_feedback_data`
- Load source data for “Ticket price data” into raw table `raw_ticket_price_data`
- Load source data for “Raw customer demographic data” into raw table `raw_customer_demographic_data`
- Load source data for “Flight destination information” into raw table `raw_flight_destination_data`
- Load source data for “Flight ticket sales” into raw table `raw_flight_ticket_sales_data`
- Load source data for “Flight Promotion” into raw table `raw_flight_promotion_data`
- Load source data for “Holiday data” into raw table `raw_holiday_data`
- Load source data for “Airline data” into raw table `raw_airline_data`
- Load source data for “Sales agent data” into raw table `raw_sales_agent_data`
- Load source data for “Ticket sales data” into raw table `raw_ticket_sales_data`
- Load source data for “Flight bookings data” into raw table `raw_flight_bookings_data`
- Load source data for “Fight destination revenue” into raw table `raw_flight_destination_revenue_data`

### Highlighting sensitive fields 

- Mark the sensitive fields (PII/PHI) in  `raw_customer_data`
- Mark the sensitive fields (PII/PHI) in  `raw_flight_schedules_data`
- Mark the sensitive fields (PII/PHI) in  `raw_customer_feedback_data`
- Mark the sensitive fields (PII/PHI) in  `raw_ticket_price_data`
- Mark the sensitive fields (PII/PHI) in  `raw_customer_demographic_data`
- Mark the sensitive fields (PII/PHI) in  `raw_flight_destination_data`
- Mark the sensitive fields (PII/PHI) in  `raw_flight_ticket_sales_data`
- Mark the sensitive fields (PII/PHI) in  `raw_flight_promotion_data`
- Mark the sensitive fields (PII/PHI) in  `raw_airline_data`
- Mark the sensitive fields (PII/PHI) in  `raw_sales_agent_data`
- Mark the sensitive fields (PII/PHI) in  `raw_ticket_sales_data`
- Mark the sensitive fields (PII/PHI) in  `raw_flight_bookings_data`
- Mark the sensitive fields (PII/PHI) in  `raw_flight_destination_revenue_data`


### Adding event logs 

- Add event logs to `raw_customer_data`
- Add event logs to `raw_flight_schedules_data`
- Add event logs to `raw_customer_feedback_data`
- Add event logs to `raw_ticket_price_data`
- Add event logs to `raw_customer_demographic_data`
- Add event logs to `raw_flight_destination_data`
- Add event logs to `raw_flight_ticket_sales_data`
- Add event logs to `raw_flight_promotion_data`
- Add event logs to `raw_airline_data`
- Add event logs to `raw_sales_agent_data`
- Add event logs to `raw_ticket_sales_data`
- Add event logs to `raw_flight_bookings_data`
- Add event logs to `raw_flight_destination_revenue_data`


### Run data profiling checks 

- Design and run data profiling checks on `raw_customer_data`
- Design and run data profiling checks on `raw_flight_schedules_data`
- Design and run data profiling checks on `raw_customer_feedback_data`
- Design and run data profiling checks on `raw_ticket_price_data`
- Design and run data profiling checks on `raw_customer_demographic_data`
- Design and run data profiling checks on `raw_flight_destination_data`
- Design and run data profiling checks on `raw_flight_ticket_sales_data`
- Design and run data profiling checks on `raw_flight_promotion_data`
- Design and run data profiling checks on `raw_airline_data`
- Design and run data profiling checks on `raw_sales_agent_data`
- Design and run data profiling checks on `raw_ticket_sales_data`
- Design and run data profiling checks on `raw_flight_bookings_data`
- Design and run data profiling checks on `raw_flight_destination_revenue_data`




| task_no | layer_name      | task                                                                                                      | task_type| completion_status |
| -----   | --------------  | ---------------                                                                                           | ---------------  | --------------- |
| DWH-001 | RAW             | Load source data for “Customer information” into raw table `raw_customer_data`                            | LOAD_TO_RAW | LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Flight schedules” into raw table `raw_flight_schedules_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Customer feedback” into raw table `raw_customer_feedback_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Ticket price data” into raw table `raw_ticket_price_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Raw customer demographic data” into raw table `raw_customer_demographic_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Flight destination information” into raw table `raw_flight_destination_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Flight ticket sales” into raw table `raw_flight_ticket_sales_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Flight Promotion” into raw table `raw_flight_promotion_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Holiday data” into raw table `raw_holiday_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Airline data” into raw table `raw_airline_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Sales agent data” into raw table `raw_sales_agent_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Ticket sales data” into raw table `raw_ticket_sales_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Flight bookings data” into raw table `raw_flight_bookings_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Load source data for “Fight destination revenue” into raw table `raw_flight_destination_revenue_data`| LOAD_TO_RAW | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_customer_data`| SENSITIVE_DATA_MARKING | SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_flight_schedules_data`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_customer_feedback_data`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_ticket_price_data`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_customer_demographic_data`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_flight_destination_data`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_flight_ticket_sales_data`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_flight_promotion_data`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_airline_data`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_sales_agent_data`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_ticket_sales_data`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_flight_bookings_data`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_flight_destination_revenue_data`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_customer_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_flight_schedules_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_customer_feedback_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_ticket_price_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_customer_demographic_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_flight_destination_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_flight_ticket_sales_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_flight_promotion_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_airline_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_sales_agent_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_ticket_sales_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_flight_bookings_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Add event logs to `raw_flight_destination_revenue_data`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_customer_data`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_flight_schedules_data`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_customer_feedback_data`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_ticket_price_data`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_customer_demographic_data`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_flight_destination_data`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_flight_ticket_sales_data`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_flight_promotion_data`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_airline_data`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_sales_agent_data`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_ticket_sales_data`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_flight_bookings_data`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-001 | RAW             | Design and run data profiling checks on `raw_flight_destination_revenue_data`|    DATA_PROFILING_CHECKS  |



## Additional notes 


...