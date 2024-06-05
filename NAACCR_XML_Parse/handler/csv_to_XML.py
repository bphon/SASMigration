#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 23:55:07 2024

@author: Jerome Corpuz 
"""

import os
import logging
import glob
import csv
import xml.etree.ElementTree as ET
from model.tumorItem import HeaderInfo, TumorItem
from model.mapping import fieldMapping, columnMapping

# Setting up basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class xmlLoadHandler:
    def __init__(self, fieldMapping, columnMapping):
        self._data_dir = os.path.join(os.getcwd(), "data")
        self._processed_dir = os.path.join(os.getcwd(), "data", "processed")
        os.makedirs(self._processed_dir, exist_ok=True)  # Ensure processed directory exists
        self._tumors = []
        self._fieldmapping = fieldMapping
        self._columnmapping = columnMapping

    def process(self):
        files = glob.glob(os.path.join(self._data_dir, "*.csv"))
        files.sort(key=os.path.getmtime)
        for filepath in files:
            logging.info(f"Processing {filepath}")
            try:
                xml_data = self.generate_xml(filepath)
                self.save_xml(xml_data, filepath)
                logging.info("XML generation complete and saved")
            except Exception as e:
                logging.error(f"Failed to process {filepath} due to {e}")

    def parse_csv(self, filepath):
        tumors_data = []
        with open(filepath, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                ti = TumorItem()
                for field, column in self._fieldmapping.items():
                    value = row.get(column, '').strip()
                    setattr(ti, field, value)
                tumors_data.append(ti)
        return tumors_data

    def parse_header_fields(self, filepath):
        with open(filepath, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.reader(file)
            header_row = next(reader)
            hi = HeaderInfo()
            for column_name in header_row:
                field_id = self._fieldmapping.get(column_name.strip())
                if field_id:
                    setattr(hi, field_id, column_name.strip())
            return hi

    def generate_xml(self, filepath):
        header_info = self.parse_header_fields(filepath)
        tumors_data = self.parse_csv(filepath)
        root = ET.Element("NaaccrData", xmlns="http://naaccr.org/naaccrxml")

        for field, value in vars(header_info).items():
            if value:
                root.set(field, value)

        for tumor_data in tumors_data:
            patient_element = ET.SubElement(root, "Patient")
            for field_id, value in vars(tumor_data).items():
                item_element = ET.SubElement(patient_element, "Item", naaccrId=field_id)
                item_element.text = value

        return ET.tostring(root, encoding='utf-8', method='xml').decode('utf-8')

    def save_xml(self, xml_data, filepath):
        base_filename = os.path.basename(filepath)
        new_filename = os.path.splitext(base_filename)[0] + ".xml"
        output_path = os.path.join(self._processed_dir, new_filename)
        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(xml_data)

handler = xmlLoadHandler(fieldMapping, columnMapping)
handler.process()
