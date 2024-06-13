import os
import logging
import sqlite3
import glob
import csv
import xml.etree.ElementTree as ET
from lxml import etree
from model.tumorItem import HeaderInfo, TumorItem
from model.mapping import fieldMapping, columnMapping

class xmlLoadHandler:
    def __init__(self):
        # Correct the paths to match your directory structure
        self._data_dir = os.path.abspath(os.path.join(os.getcwd(), "NAACCR_XML_Parse", "data"))
        self._processed_dir = os.path.join(self._data_dir, "processed")
        os.makedirs(self._processed_dir, exist_ok=True)  # Create the processed directory if it doesn't exist
        self._patients = []  # List to store patient data
        self._fieldmapping = fieldMapping  # Mapping of fields from the model
        self._columnmapping = columnMapping  # Mapping of columns from the model
        self._xsd_path = os.path.abspath(os.path.join(os.getcwd(), "NAACCR_XML_Parse", "naaccr_data_1.6.xsd"))  # Path to the XSD file for XML validation

    def Process(self):
        # Process XML files in the data directory
        xml_files = glob.glob(os.path.join(self._data_dir, "*.xml"))
        xml_files.sort(key=os.path.getmtime)  # Sort files by modification time

        logging.info(f"Data directory: {self._data_dir}")
        logging.info(f"Found XML files: {xml_files}")

        for filepath in xml_files:
            self._patients.clear()  # Clear the patients list for each file
            logging.info(f"[processing XML {filepath}]")
            try:
                tree = ET.parse(filepath)  # Parse the XML file
                root = tree.getroot()  # Get the root element of the XML
                hi = self.ParseHeaderFields(root)  # Parse header fields from the XML root
                logging.info(f"- parsing XML")
                self.ParsePatients(root, hi)  # Parse patient and tumor data
                logging.info(f"- saving patients and tumors")
                self.SavePatients()  # Save patient and tumor data to the database
                self.MoveFile(filepath)  # Move the processed file to the processed directory
            except ET.ParseError as e:
                logging.error(f"Error parsing XML file {filepath}: {e}")
                self.MoveFile(filepath, error=True)  # Move file to a separate directory for errors

        # Process CSV files in the data directory
        csv_files = glob.glob(os.path.join(self._data_dir, "*.csv"))
        csv_files.sort(key=os.path.getmtime)  # Sort files by modification time

        logging.info(f"Found CSV files: {csv_files}")

        for filepath in csv_files:
            self._patients.clear()  # Clear the patients list for each file
            logging.info(f"[processing CSV {filepath}]")
            self.ParseCsv(filepath)  # Parse patient and tumor data from CSV file
            xml_data = self.generate_xml()  # Generate XML content from parsed data
            self.save_xml(xml_data, filepath)  # Save generated XML to file
            self.MoveFile(filepath)  # Move the processed file to the processed directory

            # Validate the generated XML
            if self.validate_xml(xml_data):
                logging.info("XML Validation Successful")
            else:
                logging.error("XML Validation Failed")

    def ParseHeaderFields(self, root):
        # Parse header fields from the XML root
        hi = HeaderInfo()  # Create a new HeaderInfo instance
        nodes = [x for x in root if x.tag == "{http://naaccr.org/naaccrxml}Item"]  # Find all header items in the root
        
        for node in nodes:
            setattr(hi, node.attrib.get("naaccrId"), node.text)  # Set attributes in HeaderInfo based on XML items
        return hi

    def ParsePatients(self, root, headerInfo):
        # Parse patient and tumor data from the XML root
        patients = [x for x in root if x.tag == "{http://naaccr.org/naaccrxml}Patient"]  # Find all patient elements in the root
        
        for patient in patients:
            # Extract patient data
            patient_data = {node.attrib.get("naaccrId"): node.text.strip() if node.text else "" for node in patient if node.tag == "{http://naaccr.org/naaccrxml}Item"}
            tumors = [x for x in patient if x.tag == "{http://naaccr.org/naaccrxml}Tumor"]  # Find all tumor elements for the patient
            tumor_list = []  # List to store tumor data for the patient
            
            for tumor in tumors:
                ti = TumorItem()  # Create a new TumorItem instance
                
                # Set header fields in TumorItem
                for k, v in headerInfo.__dict__.items():
                    setattr(ti, k, v.strip() if v else "")
                
                # Set patient fields in TumorItem
                for k, v in patient_data.items():
                    setattr(ti, k, v)
                
                # Set tumor fields in TumorItem
                nodes = [x for x in tumor if x.tag == "{http://naaccr.org/naaccrxml}Item"]
                for node in nodes:
                    setattr(ti, node.attrib.get("naaccrId"), node.text.strip() if node.text else "")
                
                tumor_list.append(ti)  # Add tumor data to the list
            
            self._patients.append((patient_data, tumor_list))  # Add patient and tumor data to the patients list

    def ParseCsv(self, filepath):
        # Parse patient and tumor data from a CSV file
        with open(filepath, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)  # Create a CSV DictReader
            
            for row in reader:
                ti = TumorItem()  # Create a new TumorItem instance
                
                # Map CSV fields to TumorItem attributes
                for field, column in self._fieldmapping.items():
                    value = row.get(column, '').strip()
                    setattr(ti, field, value)
                
                self._patients.append((None, [ti]))  # Add tumor data to the patients list
        
        logging.info(f"Parsed {len(self._patients)} patients from CSV {filepath}")

    def generate_xml(self):
        # Generate XML content from patient and tumor data
        root = ET.Element("NaaccrData", xmlns="http://naaccr.org/naaccrxml")  # Create the root element
        
        for patient_data, tumors in self._patients:
            patient_element = ET.SubElement(root, "Patient")  # Create a patient element
            
            if patient_data:
                # Add patient data items to the patient element
                for field_id, value in patient_data.items():
                    item_element = ET.SubElement(patient_element, "Item", naaccrId=field_id)
                    item_element.text = value
            
            for tumor_data in tumors:
                tumor_element = ET.SubElement(patient_element, "Tumor")  # Create a tumor element
                
                # Add tumor data items to the tumor element
                for field_id, value in vars(tumor_data).items():
                    item_element = ET.SubElement(tumor_element, "Item", naaccrId=field_id)
                    item_element.text = value

        logging.info("Generated XML content from data")
        return ET.tostring(root, encoding='utf-8', method='xml').decode('utf-8')  # Convert the XML tree to a string

    def save_xml(self, xml_data, filepath):
        # Save generated XML content to a file
        base_filename = os.path.basename(filepath)  # Get the base filename from the original file path
        new_filename = os.path.splitext(base_filename)[0] + ".xml"  # Create a new filename with .xml extension
        output_path = os.path.join(self._processed_dir, new_filename)  # Get the output file path
        
        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(xml_data)  # Write the XML content to the file
        
        logging.info(f"Saved XML to {output_path}")

    def validate_xml(self, xml_data):
        # Validate the XML content against the XSD schema
        schema = etree.XMLSchema(file=self._xsd_path)  # Load the XML schema
        xml_doc = etree.fromstring(xml_data.encode('utf-8'))  # Parse the XML content
        
        return schema.validate(xml_doc)  # Validate the XML content against the schema

    def SavePatients(self):
        # Save patient and tumor data to the SQLite database
        qty = 0  # Counter for the number of saved records
        sql, keys = self.GenerateInsertSQL()  # Generate the SQL insert statement
        conn = sqlite3.connect('naaccr_data.db')  # Connect to the SQLite database
        cursor = conn.cursor()  # Create a database cursor
        
        for patient_data, tumors in self._patients:
            for tumor in tumors:
                if self.MostRecentTumor(conn, tumor):  # Check if the tumor record is the most recent
                    values = []
                    
                    for key in keys:
                        if hasattr(tumor, key):
                            attribute = getattr(tumor, key)
                            if not self.MaxLength(key, attribute):
                                attribute = None
                            values.append(attribute)
                        else:
                            values.append(None)
                    
                    self.DeleteTumor(conn, tumor)  # Delete the old tumor record if it exists
                    cursor.execute(sql, values)  # Insert the new tumor record
                    conn.commit()  # Commit the transaction
                    qty += 1  # Increment the counter
        
        logging.info(f"[number of XML/CSV items saved]: {qty}")

    def SaveTumors(self):
        # Implementation for saving tumor data if separate logic is required
        pass

    def MostRecentTumor(self, conn, tumor):
        # Check if the tumor record is the most recent in the database
        params = (
            getattr(tumor, "medicalRecordNumber", getattr(tumor, "patientIdNumber", "")),
            getattr(tumor, "tumorRecordNumber"), 
            getattr(tumor, "registryId"), 
            getattr(tumor, "dateCaseReportExported", getattr(tumor, "dateCaseLastChanged", ""))
        )
        
        sql = """SELECT * FROM NAACCR_DATA 
                 WHERE MEDICAL_RECORD_NUMBER_N2300 = ? 
                   AND TUMOR_RECORD_NUMBER_N60 = ? 
                   AND REGISTRY_ID_N40 = ?
                   AND DATE_CASE_REPORT_EXPORT_N2110 > ?"""
        
        cmd = conn.cursor()  # Create a database cursor
        cmd.execute(sql, params)  # Execute the query with parameters
        row = cmd.fetchone()  # Fetch one record
        
        return row is None  # Return True if no record is found, indicating this is the most recent tumor

    def DeleteTumor(self, conn, tumor):
        # Delete an old tumor record from the database
        params = (
            getattr(tumor, "medicalRecordNumber", getattr(tumor, "patientIdNumber", "")),
            getattr(tumor, "tumorRecordNumber"), 
            getattr(tumor, "registryId")
        )
        
        sql = """DELETE FROM NAACCR_DATA 
                 WHERE MEDICAL_RECORD_NUMBER_N2300 = ? 
                   AND TUMOR_RECORD_NUMBER_N60 = ? 
                   AND REGISTRY_ID_N40 = ?"""
        
        cmd = conn.cursor()  # Create a database cursor
        cmd.execute(sql, params)  # Execute the delete query with parameters
        conn.commit()  # Commit the transaction

    def GenerateInsertSQL(self):
        # Generate the SQL insert statement for tumor data
        keys = []
        fields1 = ""
        fields2 = ""
        
        for key in self._fieldmapping:
            keys.append(key)
            if fields1 == "":
                fields1 = self._fieldmapping[key]
                fields2 = "?"
            else:
                fields1 = fields1 + ", " + self._fieldmapping[key]
                fields2 = fields2 + ", ?"
        
        sql = f"INSERT INTO NAACCR_DATA ({fields1}) VALUES ({fields2})"  # Create the SQL insert statement
        return (sql, keys)

    def MoveFile(self, filepath, error=False):
        # Move the processed file to the processed directory or an error directory if parsing failed
        target_dir = self._processed_dir
        if error:
            target_dir = os.path.join(self._processed_dir, "error")
            os.makedirs(target_dir, exist_ok=True)
        
        os.rename(filepath, os.path.join(target_dir, os.path.basename(filepath)))  # Move the file

    def MaxLength(self, key, attribute):
        # Check if the attribute value exceeds the maximum length defined in the column mapping
        dbfield = self._fieldmapping[key]
        maxlen = self._columnmapping[dbfield]
        
        if len(attribute) > maxlen:
            logging.warning(f"value too long - column: {key}, value: {attribute}, maxlen: {maxlen}")
            return False
        
        return True

if __name__ == "__main__":
    # Set up logging and start processing files
    logging.basicConfig(level=logging.DEBUG)
    handler = xmlLoadHandler()
    handler.Process()
