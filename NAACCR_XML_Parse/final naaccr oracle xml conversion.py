# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 15:38:43 2024

@author: lihuang
"""
import oracledb
import csv
import xml.etree.ElementTree as ET
from lxml import etree
import glob
import pandas as pd
import sys
import os
sys.path.append('H:/R_extras/')

from configfile import *



xsd_path = 'naaccr_data_1.6.xsd'
xml_path = 'naaccr-dictionary-230.xml'

xml_doc = etree.XML(open(xml_path, 'r').read())

xsd_doc = etree.XML(open(xsd_path, 'r').read())
xsd_schema = etree.XMLSchema(xsd_doc)




def parse_naaccr_dictionary(dictionary_path):
        # Parse the NAACCR dictionary XML
    tree = etree.parse(dictionary_path)
    root = tree.getroot()
    patient_items = set()
    tumor_items = set()

        # Extract Patient and Tumor item definitions
    for item in root.xpath("//ns:ItemDef", namespaces={'ns': 'http://naaccr.org/naaccrxml'}):
        naaccr_id = item.get("naaccrId")
        parent_tag = item.get("parentXmlElement")
        if parent_tag == "Patient":
            patient_items.add(naaccr_id)
        elif parent_tag == "Tumor":
            tumor_items.add(naaccr_id)
    
    return patient_items, tumor_items




oracledb.init_oracle_client()
connection = oracledb.connect(user= f"{ORACLE_USERNAME}[ALBERTA_CANCER_REGISTRY_ANLYS]", password=ORACLE_PASSWORD, dsn= ORACLE_TNS_NAME)

qry_cursor = connection.cursor()
qry_cursor.execute('SELECT * FROM NAACCR_DATA_2022')
rows_fetched  = qry_cursor.fetchall()
cols = [desc[0] for desc in qry_cursor.description]

qry_cursor.close()    
connection.close()
dbf = pd.DataFrame(rows_fetched, columns = cols)

dbf['ind_pt_dup'] = dbf.duplicated(subset = 'patientIdNumber', keep = 'first') # get the duplicated patient a flag 'True'
dbf_nonum = dbf.applymap(str)
dbf_dict = dbf_nonum.to_dict(orient='records')

  
  
dictionary_path = os.path.abspath(os.path.join(os.getcwd(), "naaccr-dictionary-230.xml"))
patient_items, tumor_items = parse_naaccr_dictionary(dictionary_path)


def generate_sample_xml(oracle_data):
        # Create the root element of the XML
        root = etree.Element("NaaccrData",
                             xmlns="http://naaccr.org/naaccrxml",
                             baseDictionaryUri="http://naaccr.org/naaccrxml/naaccr-dictionary-230.xml",
                             recordType="I",
                             specificationVersion="1.6")

        # Add global items
        global_items = [
            {"naaccrId": "registryType", "value": "1"},
            {"naaccrId": "recordType", "value": "I"},
            {"naaccrId": "naaccrRecordVersion", "value": "230"},
            {"naaccrId": "registryId", "value": "0022004800"}
        ]
        for item in global_items:
            item_element = etree.SubElement(root, "Item", naaccrId=item["naaccrId"])
            item_element.text = item["value"]


        # Parse the NAACCR dictionary to get patient and tumor item mappings
        dictionary_path = os.path.abspath(os.path.join(os.getcwd(),  "naaccr-dictionary-230.xml"))
    
        patient_items, tumor_items = parse_naaccr_dictionary(dictionary_path)

        for patient_data in oracle_data:
           
            # Adding Patient Items
            if patient_data['ind_pt_dup'] == "False":
                patient_element = etree.SubElement(root, "Patient")
                for naaccr_id, value in patient_data.items():
                    if naaccr_id in patient_items and value != "None":
                        item_element = etree.SubElement(patient_element, "Item", naaccrId=naaccr_id)
                        item_element.text = value
                        
            # Create a Tumor element
            tumor_element = etree.SubElement(patient_element, "Tumor")
            for naaccr_id, value in patient_data.items():
                
                if naaccr_id in tumor_items and value != "None":
                    item_element = etree.SubElement(tumor_element, "Item", naaccrId=naaccr_id)
                    item_element.text = value

        # Convert the XML tree to a string
        xml_data = etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8")
        return xml_data


def save_xml( xml_data, filename):
    output_path = os.path.abspath(filename)
    with open(output_path, 'wb') as file:
        file.write(xml_data)
        

def validate_xml(xml_path, xsd_path):
    # Open and read the XSD file
    with open(xsd_path, 'rb') as xsd_file:
        xsd_content = xsd_file.read()
    # Parse the XSD content
    xsd_doc = etree.XML(xsd_content)
    xsd_schema = etree.XMLSchema(xsd_doc)

    # Open and read the XML file
    with open(xml_path, 'rb') as xml_file:
        xml_content = xml_file.read()
    xml_doc = etree.XML(xml_content)

    # Validate the XML against the schema
    is_valid = xsd_schema.validate(xml_doc)
    
    return is_valid
    
oracle_xml = generate_sample_xml(dbf_dict)


save_xml(oracle_xml, 'oracle_50.xml')

validate_xml('oracle_xml.xml', xsd_path)

# Code to read in xml file
with open('oracle_xml.xml', 'rb') as xml_file:
    xml_content = xml_file.read()
    xml_doc = etree.XML(xml_content)
    
tree = ET.parse('oracle_xml.xml')
    
data = []
for patient in tree.findall('.//Patient'):  # Assuming 'person' is the element containing your data
    temp_data = {
        'patientIdNumber': patient.find('patientIdNumber').text,
        'sex': patient.find('sex').text
    }
    
    data.append(temp_data)

## Test

patient_set = dbf_nonum['patientIdNumber'].unique()



root = etree.Element("naaccr_data",
                     xmlns="http://naaccr.org/naaccrxml",
                     baseDictionaryUri="http://naaccr.org/naaccrxml/naaccr-dictionary-230.xml",
                     recordType="I",
                     specificationVersion="1.6")

# Add global items
global_items = [
    {"naaccrId": "registryType", "value": "1"},
    {"naaccrId": "recordType", "value": "I"},
    {"naaccrId": "naaccrRecordVersion", "value": "230"},
    {"naaccrId": "registryId", "value": "0022004800"}
]
for item in global_items:
    item_element = etree.SubElement(root, "Item", naaccrId=item["naaccrId"])
    item_element.text = item["value"]


# Parse the NAACCR dictionary to get patient and tumor item mappings
dictionary_path = os.path.abspath(os.path.join(os.getcwd(),  "naaccr-dictionary-230.xml"))

patient_items, tumor_items = parse_naaccr_dictionary(dictionary_path)

for patient_data in oracle_data:
    # Create a Patient element
    patient_element = etree.SubElement(root, "Patient")

    # Adding Patient Items
    for naaccr_id, value in patient_data.items():
        if naaccr_id in patient_items and value != "None":
            item_element = etree.SubElement(patient_element, "Item", naaccrId=naaccr_id)
            item_element.text = value

    # Create a Tumor element
    tumor_element = etree.SubElement(patient_element, "Tumor")

    for naaccr_id, value in patient_data.items():
        if naaccr_id in tumor_items and value != "None":
            item_element = etree.SubElement(tumor_element, "Item", naaccrId=naaccr_id)
            item_element.text = value

# Convert the XML tree to a string
xml_data = etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8")

def read_oracle_data(oracle_data):
    oracle_data = oracle_data.to_dict('records')
    return oracle_data