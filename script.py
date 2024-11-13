# ETL Project
import math
import json
import psycopg2
import pandas as pd
import psycopg2.extras as extras
import xml.etree.ElementTree as ET
from psycopg2.extras import execute_batch

# EXTRACT
## Read the data from files
csv_data = pd.read_csv("./user_data_23_4.csv")
csv_data = csv_data.to_dict("records")

# Read the JSON file
json_data = pd.read_json("./user_data_23_4.json")
json_data = json_data.to_dict("records")



# Read the txt file
with open("./user_data_23_4.txt", "r") as f:
    text_file = f.readlines()
    
    text_data = []
    
    for i in text_file:
        text_data.append(i)

# Read the xml file
xml_data = []
tree = ET.parse("./user_data_23_4.xml")
xml_root = tree.getroot()

for child in xml_root:
    xml_data.append(child.attrib)


# TRANSFORM
# Aggregate the data from each data source into a single source
# Using the firstName, lastName, and age as the primary keys
new_data_records = []

# Aggregate the JSON_DATA and CSV_DATA
for i in json_data:
    for j in csv_data:
        if i["firstName"] == j["First Name"] and i["lastName"] == j["Second Name"] and i["age"] == j["Age (Years)"]:
            # Set the debt value to prevent errors
            computed_debt = 0
            if type(i["debt"]) == dict:
                computed_debt = float(i["debt"]["amount"])
            elif type(i["debt"]) == str:
                computed_debt = float(i["debt"])
            elif type(i["debt"]) == float:
                computed_debt = -1
            else:
                computed_debt = 0
            
            new_data_records.append(
                {
                    # Add CSV file data
                    "first_name": i["firstName"],
                    "last_name": i["lastName"],
                    "age": i["age"],
                    "iban": i["iban"],
                    "credit_card_number": i["credit_card_number"],
                    "credit_card_security_code": i["credit_card_security_code"],
                    "credit_card_start_date": i["credit_card_start_date"],
                    "credit_card_end_date": i["credit_card_end_date"],
                    "address_main": i["address_main"],
                    "address_city": i["address_city"],
                    "address_postcode": i["address_postcode"],
                    "debt": computed_debt,
                    # Add JSON file data
                    "sex": j["Sex"],
                    "vehicle_make": j["Vehicle Make"],
                    "vehicle_model": j["Vehicle Model"],
                    "vehicle_year": j["Vehicle Year"],
                    "vehicle_type": j["Vehicle Type"],
                }
            )

# Aggregate the NEW_DATA_RECORDS with the XML_DATA
for key, value in enumerate(new_data_records):
    for j in xml_data:
        # FOR SOME REASON NO AGE IN THE XML FILE MATCHES THE AGE IN THE NEW_DATA_RECORDS
        if new_data_records[key]["first_name"] == j["firstName"] and new_data_records[key]["last_name"] == j["lastName"] and new_data_records[key]["age"] == int(j["age"]):
            new_data_records[key]["retired"] = j["retired"]
            new_data_records[key]["dependants"] = j["dependants"]
            new_data_records[key]["marital_status"] = j["marital_status"]
            new_data_records[key]["salary"] = j["salary"]
            new_data_records[key]["pension"] = j["pension"]
            new_data_records[key]["company"] = j["company"]
            new_data_records[key]["commute_distance"] = j["commute_distance"]

# Perform Operations from in the .txt file
for index, customer in enumerate(new_data_records):
    # Valerie Ellis -> Security Code Wrong -> Change security code to 762
    if customer["first_name"] == "Valerie" and customer["last_name"] == "Ellis":
        # UPDATE it
        new_data_records[index]["credit_card_security_code"] = 762
    
    # West was promoted -> £2100 salary bump -> Company name: Williams-Wheeler -> Other name Charlie
    if customer["company"] == "Williams-Wheeler":
        # UPDATE it
        new_data_records[index]["salary"] = int(new_data_records[index]["salary"]) + 2100
    
    # Names: Charlie Short -> New age: 52
    if customer["first_name"] == "Charlie" and customer["last_name"] == "Short":
        # UPDATE it
        new_data_records[index]["age"] = 52
    
    # Name: Martin -> Gender: Male -> 0.15% increment to £22896 -> Pension: £22896
    if int(customer["pension"]) == 22896:
        # UPDATE it
        new_data_records[index]["pension"] = float(customer["pension"]) + (float(customer["pension"]) * (0.15/100))

# LOAD
# Connect to our database
conn = psycopg2.connect(host="localhost", dbname="postgres",    user="postgres", password="password", port=5432)

cur = conn.cursor()

# Create the customer Table
cur.execute("""
    CREATE TABLE IF NOT EXISTS customer (
        first_name TEXT,
        last_name TEXT,
        age INT,
        iban TEXT,
        credit_card_number BIGINT,
        credit_card_security_code TEXT,
        credit_card_start_date TEXT,
        credit_card_end_date TEXT,
        address_main TEXT,
        address_city TEXT,
        address_postcode TEXT,
        debt NUMERIC,
        sex TEXT,
        vehicle_make TEXT,
        vehicle_model TEXT,
        vehicle_year INT,
        vehicle_type TEXT,
        retired BOOLEAN,
        dependants TEXT,
        marital_status TEXT,
        salary NUMERIC,
        pension NUMERIC,
        company TEXT,
        commute_distance NUMERIC
    )
"""
)

# Convert data to list of tuples for easy insertion
values = [
    (
        row['first_name'], row['last_name'], row['age'], row['iban'], row['credit_card_number'],
        row['credit_card_security_code'],
        row['credit_card_start_date'],
        row['credit_card_end_date'], row['address_main'], row['address_city'], row['address_postcode'],
        row['debt'], row['sex'], row['vehicle_make'], row['vehicle_model'], row['vehicle_year'],
        row['vehicle_type'], row['retired'], row['dependants'], row['marital_status'],
        row['salary'], row['pension'], row['company'], row['commute_distance']
    )
    for row in new_data_records
]

# SQL insert query
insert_query = (
    """
    INSERT INTO customer (
        first_name, last_name, age, iban, credit_card_number, credit_card_security_code, credit_card_start_date, credit_card_end_date,
        address_main, address_city, address_postcode, debt, sex, vehicle_make, vehicle_model,
        vehicle_year, vehicle_type, retired, dependants, marital_status, salary, pension,
        company, commute_distance
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""
)

# Add the new data records to the table
execute_batch(cur, insert_query, values)

# Commit operations to DB
conn.commit()

# Close Cursor and Connection
cur.close()
conn.close()