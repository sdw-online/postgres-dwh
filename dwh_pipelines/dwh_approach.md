# Approach 




## Source data 

Here are some of the tables gathered from the travel source systems (databases, CRMs and other tools):

- Customer information
- Flight schedules
- Customer feedback
- Ticket price data
- Raw customer demographic data
- Flight destination information
- Flight ticket sales
- Flight Promotion
- Holiday data
- Airline data


## Layers 

* Raw layer - for storing source data in its original state 
* Staging layer - for cleaning and framing raw data in a suitable format for pre-computing
* Semantic layer - for pre-computing staged data with business logic to create single version of truth 
* Data warehouse layer - for exposing single version of truth to downstream users as the DWH
* Governance layer - for establishing processes, practices and policies for managing the DWH's data  
* Orchestration layer - for scheduling pipeline tasks and managing their dependencies 




## Raw layer

### Macro tasks

- Load source tables into raw tables
- Highlight sensitive fields
- Add event logging
- Run data profiling checks


### Micro tasks


***

## Staging layer 

### Macro tasks

- Load raw data into staging tables
- Design transformation strategy
- Design DQ constraints and QA tests
- Execute transformation strategy
- Add surrogate keys to staging tables
- Execute DQ tests

### Micro tasks


***

## Semantic layer  

### Macro tasks
- Load staging to semantic tables
- Add business rules to semantic tables
- Create fact and dimension tables
- Define cardinality between tables (via ERD if possible)
- Create data dictionary for the tables

### Micro tasks


***

## Data warehouse layer 

### Macro tasks
- Create aggregated views using the fact and dimension tables
- Document the code in each layer to reduce single point of failure risk
- Conduct regular maintenance activities e.g. performance tuning, backups, system updates

### Micro tasks


***

## Governance layer 

### Macro tasks
- Understand the members of the analytics team that require access to the DWH
- Create custom roles using a role-based access control
- Grant table ownership rights to the delegated roles
- Grant the appropriate schema usage rights to each custom role
- Grant privileges to each custom role around each member's responsibilities in the analytics team
- Create row security policies to control what data is visible to each role within their authorized tables

### Micro tasks


***

## Orchestration layer 

### Macro tasks


- Set up workflows and task dependencies in Prefect
- Setup CI/CD pipelines in GitHub Actions to automatically test and deploy changes made to the DWH

### Micro tasks


***
