# -*- coding: utf-8 -*-
"""
Created on Wed Jun 19 09:39:29 2024

@author: user
"""
import os
import xml.etree.ElementTree as et
import cx_Oracle
import logging
from utility import Utility  # Assuming Utility is in a file named utility.py

class xml_to_oracle:
    

    def __init__(self):
        # Initialize utility and configure logging
        self._util = Utility()
        self._util.ConfigureLogging()
    
    def process(self, directory, dsn, user, password):
        try:
            logging.info('*** Start ***')

            logging.info("Finding XML file...")
            xml_file = self.find_xml_file(directory)
            if not xml_file:
                logging.error("No XML file found in the specified directory.")
                return

            logging.info(f"Processing file: {xml_file}")

            logging.info("Parsing XML data...")
            data = self.parse_xml(xml_file)

            logging.info("Connecting to database and inserting data...")
            self.insert_data_to_db(data, dsn, user, password)

            logging.info('*** Stop ***')
        except Exception as e:
            logging.exception("Exception occurred during processing")

    def find_xml_file(self, directory):
        for file in os.listdir(directory):
            if file.endswith('.xml'):
                return os.path.join(directory, file)
        return None

    def parse_xml(self, xml_file):
        tree = et.parse(xml_file)
        root = tree.getroot()

        # Define the namespaces
        namespaces = {'ns': 'http://naaccr.org/naaccrxml'}

        # Extract the global items
        global_items = {}
        for item in root.findall('ns:Item', namespaces):
            naaccr_id = item.get('naaccrId')
            value = item.text
            global_items[naaccr_id] = value

        # Extract patient and tumor data
        data = []
        for patient in root.findall('ns:Patient', namespaces):
            patient_data = global_items.copy()
            for item in patient.findall('ns:Item', namespaces):
                naaccr_id = item.get('naaccrId')
                value = item.text
                patient_data[naaccr_id] = value

            for tumor in patient.findall('ns:Tumor', namespaces):
                tumor_data = patient_data.copy()
                for item in tumor.findall('ns:Item', namespaces):
                    naaccr_id = item.get('naaccrId')
                    value = item.text
                    tumor_data[naaccr_id] = value
                data.append(tumor_data)

        return data

    def insert_data_to_db(self, data, dsn, user, password):
        connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)
        cursor = connection.cursor()

        # Insert data into the table
        insert_sql = """
        INSERT INTO naaccr_data ({}) VALUES ({})
        """.format(', '.join(data[0].keys()), ', '.join([f":{key}" for key in data[0].keys()]))

        for row in data:
            cursor.execute(insert_sql, row)

        # Commit the transaction
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()

    

# Example usage
directory = os.path.join(os.getcwd(), 'nacccr_generated_xml')
dsn = "your_dsn"
user = "your_username"
password = "your_password"

parser = xml_to_oracle()
parser.process(directory, dsn, user, password)


if __name__ == "__main__":
    # Run the application
    app = save_to_oracle()
    app.Process()
