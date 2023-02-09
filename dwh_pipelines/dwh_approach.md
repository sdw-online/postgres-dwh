# Approach 



# Objective 

To create a centralized platform for the analytics team to analyze customer and flight information. This platform aims to provide insights that will support the team in making informed decisions for enhancing the customer's travel experience. This means the analytics team will require access to a wealth of data from one location (i.e. the data warehouse) allowing them to make data-driven decisions that benefit the company, resulting in a more efficient process that improves customer satisfaction. 


## Source data 

Here are some of the tables gathered from the travel source systems (databases, CRMs and other tools):

- Customer information              ------------ [x]
- Flight schedules                  ------------ [x]
- Ticket price data                 ------------ [x]
- Flight bookings data              ------------ [x]
- Customer feedback                 ------------ [x]
- Raw customer demographic data     ------------ []
- Flight destination information    ------------ [x]
- Flight ticket sales               ------------ [x]
- Flight Promotions & Deals         ------------ [x]
- Holiday data                      ------------ []
- Airline data                      ------------ []
- Sales agent data                  ------------ [x]
- Fight destination revenue         ------------ []
- Accommodation bookings data         ------------ [x]


## Layers 

Here are the different layers that make up the proposed data warehouse solution in Postgres:

* Raw layer - for storing source data in its original state 
* Staging layer - for cleaning and framing raw data in a suitable format for pre-computing
* Semantic layer - for pre-computing staged data with business logic to create single version of truth 
* Data warehouse layer - for displaying the single version of truth in a unified manner to the downstream users 
* Governance layer - for establishing processes, practices and policies for managing the DWH's data  
* Orchestration layer - for scheduling and managing pipeline tasks and their dependencies 




## Raw layer

### Macro tasks

- Load source tables into raw tables
- Highlight sensitive fields
- Add event logging
- Run data profiling checks






***

## Staging layer 

### Macro tasks

- Load raw data into staging tables
- Design transformation strategy
- Design DQ constraints and QA tests
- Execute transformation strategy
- Execute DQ tests
- Create DEV and PROD environments (schemas)




***

## Semantic layer  

### Macro tasks
- Load staging to semantic tables
- Add surrogate keys to semantic tables
- Add business rules to semantic tables
- Add date dimension table 
- Add the date foreign keys to the relevant dim tables 
- Define cardinality between tables (via ERD if possible)
- Create data dictionary for the tables
- Create DEV and PROD environments (schemas)




***

## Data warehouse layer 

### Macro tasks
- Create fact and dimension tables (dimensional modelling)
- Create aggregated views using the fact and dimension tables
- Document the code in each layer to reduce single point of failure risk
- Conduct regular maintenance activities e.g. performance tuning, backups, system updates




***

## Governance layer 

### Macro tasks
- Understand the members of the analytics team that require access to the DWH
- Create custom roles using a role-based access control
- Grant table ownership rights to the delegated roles
- Grant the appropriate schema usage rights to each custom role
- Grant privileges to each custom role around each member's responsibilities in the analytics team
- Create row security policies to control what data is visible to each role within their authorized tables




***

## Orchestration layer 

### Macro tasks


- Set up workflows and task dependencies in Prefect
- Setup CI/CD pipelines in GitHub Actions to automatically test and deploy changes made to the DWH




***
