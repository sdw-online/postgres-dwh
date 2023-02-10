# Task Checklist 

This is a recollection of all the micro tasks for each DWH layer




### Table 

| task_no | layer_name      | task                                                                                                      | task_type| completion_status |
| -----   | --------------  | ---------------                                                                                           | ---------------  | --------------- |
| DWH-001 | RAW             | Load source data for “Customer information” into raw table `raw_customer_tbl`                            | SRC_TO_RAW  | COMPLETED |
| DWH-002 | RAW             | Load source data for “Flight schedules” into raw table `raw_flight_schedules_tbl`| SRC_TO_RAW | COMPLETED |
| DWH-003 | RAW             | Load source data for “Customer feedback” into raw table `raw_customer_feedback_tbl`| SRC_TO_RAW | COMPLETED |
| DWH-004 | RAW             | Load source data for “Ticket price data” into raw table `raw_ticket_price_tbl`| SRC_TO_RAW | COMPLETED |
| DWH-005 | RAW             | Load source data for “Raw customer demographic data” into raw table `raw_customer_demographic_tbl`| SRC_TO_RAW | NOT_STARTED |
| DWH-006 | RAW             | Load source data for “Flight destination information” into raw table `raw_flight_destination_tbl`| SRC_TO_RAW | NOT_STARTED |
| DWH-007 | RAW             | Load source data for “Flight ticket sales” into raw table `raw_flight_ticket_sales_tbl`| SRC_TO_RAW | COMPLETED |
| DWH-008 | RAW             | Load source data for “Flight Promotion” into raw table `raw_flight_promotion_tbl`| SRC_TO_RAW | COMPLETED |
| DWH-009 | RAW             | Load source data for “Holiday data” into raw table `raw_holiday_tbl`| SRC_TO_RAW | NOT_STARTED |
| DWH-010 | RAW             | Load source data for “Airline data” into raw table `raw_airline_tbl`| SRC_TO_RAW | NOT_STARTED |
| DWH-011 | RAW             | Load source data for “Sales agent data” into raw table `raw_sales_agent_tbl`| SRC_TO_RAW | NOT_STARTED |
| DWH-012 | RAW             | Load source data for “Ticket sales data” into raw table `raw_ticket_sales_tbl`| SRC_TO_RAW | NOT_STARTED |
| DWH-013 | RAW             | Load source data for “Flight bookings data” into raw table `raw_flight_bookings_tbl`| SRC_TO_RAW | NOT_STARTED |
| DWH-014 | RAW             | Load source data for “Fight destination revenue” into raw table `raw_flight_destination_revenue_tbl`| SRC_TO_RAW | NOT_STARTED |
| DWH-015 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_customer_tbl`| SENSITIVE_DATA_MARKING  | COMPLETED |
| DWH-016 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_flight_schedules_tbl`| SENSITIVE_DATA_MARKING | COMPLETED |
| DWH-017 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_customer_feedback_tbl`| SENSITIVE_DATA_MARKING | COMPLETED |
| DWH-018 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_ticket_price_tbl`| SENSITIVE_DATA_MARKING | COMPLETED |
| DWH-019 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_customer_demographic_tbl`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-020 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_flight_destination_tbl`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-021 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_flight_ticket_sales_tbl`| SENSITIVE_DATA_MARKING | COMPLETED |
| DWH-022 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_flight_promotion_tbl`| SENSITIVE_DATA_MARKING | COMPLETED |
| DWH-023 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_airline_tbl`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-024 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_sales_agent_tbl`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-025 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_ticket_sales_tbl`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-026 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_flight_bookings_tbl`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-027 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_flight_destination_revenue_tbl`| SENSITIVE_DATA_MARKING | NOT_STARTED |
| DWH-028 | RAW             | Add event logs to `raw_customer_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | COMPLETED |
| DWH-029 | RAW             | Add event logs to `raw_flight_schedules_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | COMPLETED |
| DWH-030 | RAW             | Add event logs to `raw_customer_feedback_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | COMPLETED |
| DWH-031 | RAW             | Add event logs to `raw_ticket_price_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | COMPLETED |
| DWH-032 | RAW             | Add event logs to `raw_customer_demographic_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-033 | RAW             | Add event logs to `raw_flight_destination_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-034 | RAW             | Add event logs to `raw_flight_ticket_sales_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | COMPLETED |
| DWH-035 | RAW             | Add event logs to `raw_flight_promotion_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | COMPLETED |
| DWH-036 | RAW             | Add event logs to `raw_airline_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-037 | RAW             | Add event logs to `raw_sales_agent_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-038 | RAW             | Add event logs to `raw_ticket_sales_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-039 | RAW             | Add event logs to `raw_flight_bookings_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-040 | RAW             | Add event logs to `raw_flight_destination_revenue_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | NOT_STARTED |
| DWH-041 | RAW             | Design and run data profiling checks on `raw_customer_tbl`|    DATA_PROFILING_CHECKS  | COMPLETED |
| DWH-042 | RAW             | Design and run data profiling checks on `raw_flight_schedules_tbl`|    DATA_PROFILING_CHECKS  | COMPLETED |
| DWH-043 | RAW             | Design and run data profiling checks on `raw_customer_feedback_tbl`|    DATA_PROFILING_CHECKS  | COMPLETED |
| DWH-044 | RAW             | Design and run data profiling checks on `raw_ticket_price_tbl`|    DATA_PROFILING_CHECKS  | COMPLETED |
| DWH-045 | RAW             | Design and run data profiling checks on `raw_customer_demographic_tbl`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-046 | RAW             | Design and run data profiling checks on `raw_flight_destination_tbl`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-047 | RAW             | Design and run data profiling checks on `raw_flight_ticket_sales_tbl`|    DATA_PROFILING_CHECKS  | COMPLETED |
| DWH-048 | RAW             | Design and run data profiling checks on `raw_flight_promotion_tbl`|    DATA_PROFILING_CHECKS  | COMPLETED |
| DWH-049 | RAW             | Design and run data profiling checks on `raw_airline_tbl`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-050 | RAW             | Design and run data profiling checks on `raw_sales_agent_tbl`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-051 | RAW             | Design and run data profiling checks on `raw_ticket_sales_tbl`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-052 | RAW             | Design and run data profiling checks on `raw_flight_bookings_tbl`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-053 | RAW             | Design and run data profiling checks on `raw_flight_destination_revenue_tbl`|    DATA_PROFILING_CHECKS  | NOT_STARTED |
| DWH-054 | RAW             | Load source data for “Accommodation bookings” into raw table `raw_accommodation_bookings_tbl`| SRC_TO_RAW | COMPLETED |
| DWH-055 | RAW             | Mark the sensitive fields (PII/PHI) in `raw_accommodation_bookings_tbl`| SENSITIVE_DATA_MARKING  | COMPLETED |
| DWH-056 | RAW             | Add event logs to `raw_accommodation_bookings_tbl`|    LOGGING_RAW_LEVEL_EVENTS  | COMPLETED |
| DWH-057 | RAW             | Design and run data profiling checks on `raw_accommodation_bookings_tbl`|    DATA_PROFILING_CHECKS  | COMPLETED |



| DWH-xxx | STAGING             | xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx |    xxxxxxxxxxxxxxxxxxxxxxxxxx  | NOT_STARTED |


