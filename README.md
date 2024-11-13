# ETL Project - Unifying the Data Landscape

## Project Overview

In today's data-driven world, accessing an integrated view of scattered data is essential for strategic insight. This project transforms data from multiple formats—CSV, JSON, TXT, and XML—into a single, structured view that's then pushed to a central data store. The result is a unified dataset ready for analytics, business intelligence, and decision-making.

## Dataset and Its Features

Each data source brings unique information to the final product, and together, they create a holistic view of our user profiles.

### Data Sources and Fields

---

### CSV File: Core User Information

The CSV file provides foundational user attributes:

- **First Name**: User's first name
- **Second Name**: User's last name
- **Age (Years)**: User's age in years
- **Sex**: Gender of the user
- **Vehicle Make**: Vehicle brand
- **Vehicle Model**: Model of the user's vehicle
- **Vehicle Year**: Manufacturing year of the vehicle
- **Vehicle Type**: Type or category of the vehicle

---

### JSON File: Financial and Contact Information

The JSON file enriches user profiles with financial and contact details:

- **firstName** and **lastName**: User's full name
- **age**: User's age in years
- **iban**: User's IBAN number for banking information
- **credit_card_number** and **credit_card_security_code**: Card details for billing and verification
- **credit_card_start_date** and **credit_card_end_date**: Validity period of the credit card
- **address_main**, **address_city**, and **address_postcode**: User's address details
- **debt**: Outstanding debt, if any

---

### TXT File: Operational Instructions

The TXT file provides essential instructions for handling certain records, including specific updates and adjustments based on business rules.

---

### XML File: Additional Personal and Employment Details

The XML file complements user profiles with additional personal and employment-related fields:

- **firstName** and **lastName**: User's full name
- **age** and **sex**: User's age and gender
- **retired**: Indicates retirement status
- **dependants**: Number of dependents
- **marital_status**: User's marital status
- **salary** and **pension**: User's salary and pension details
- **company**: Company name where the user is employed
- **commute_distance**: Commute distance to the company
- **address_postcode**: User's postal code

---

## ETL Process: From Raw Data to Integrated Dataset

### 1. **Extract**: Gathering Data from Multiple Sources

In the extraction stage, data is pulled from all four sources (CSV, JSON, TXT, and XML), each offering unique aspects of user information. This stage consolidates the data into dictionary structures, laying the foundation for transformation.

### 2. **Transform**: Merging and Refining Data

The transformation stage merges data from each source, aligning user records by `firstName`, `lastName`, and `age` to create a single, cohesive dataset.

- **Aggregation**: Core attributes from CSV and JSON are combined, and XML attributes are integrated where available.
- **Data Cleansing**: Missing values (e.g., `debt` transformed to appropriate values) and instructions from the TXT file (e.g., security code and salary adjustments) ensure data consistency and correctness.

### 3. **Load**: Storing the Unified Data

The final, transformed data is loaded into a PostgreSQL database within a structured `customer` table. This table holds all essential fields across sources, ready for advanced querying and analysis.

## Schema: `customer` Table

| Field                       | Type        | Description                                               |
|-----------------------------|-------------|-----------------------------------------------------------|
| first_name, last_name       | TEXT        | User's name                                               |
| age                         | INT         | User's age                                                |
| iban                        | TEXT        | IBAN for banking information                              |
| credit_card_number          | BIGINT      | User's credit card number                                 |
| credit_card_security_code   | TEXT        | Security code                                             |
| credit_card_start_date, end | TEXT        | Validity period of the card                               |
| address_main, city, postcode| TEXT        | Address fields                                            |
| debt                        | NUMERIC     | Outstanding debt                                          |
| sex                         | TEXT        | User's gender                                             |
| vehicle_make, model, year   | TEXT/INT    | Vehicle information                                       |
| retired, dependants         | BOOLEAN/TEXT| Retirement status and dependents                          |
| marital_status              | TEXT        | Marital status                                            |
| salary, pension             | NUMERIC     | Income and retirement savings                             |
| company                     | TEXT        | Employer name                                             |
| commute_distance            | NUMERIC     | Commute distance to work                                  |

## Conclusion

The ETL process transforms disjointed data into a unified and enriched user dataset, enabling analytics and insights from a single, comprehensive view. With robust data processing and transformation, this project lays a strong foundation for high-quality data integration, ready to support strategic decisions.
