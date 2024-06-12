import os
import logging
from utility import Utility
from handler.xmlLoadHandler import xmlLoadHandler
from repository import Repository
from lxml import etree
import oracledb

class App:
    def __init__(self):
        self._util = Utility()
        self._util.ConfigureLogging()

    def Process(self):
        try:
            logging.info('*** Start ***')
            self.Config()
            Repository().Configure()    # create local db
            xmlLoadHandler().Process()  # read CSV files and save them to local db
            
            # Validate XML against XSD
            xml_path = os.path.abspath(r'G:\OneDrive\Desktop\AHS Work\SASMigration\NAACCR_XML_Parse\naaccr-dictionary-230.xml')
            xsd_path = os.path.abspath(r'G:\OneDrive\Desktop\AHS Work\SASMigration\NAACCR_XML_Parse\naaccr_data_1.6.xsd')

            logging.debug(f"XML path: {xml_path}")
            logging.debug(f"XSD path: {xsd_path}")

            # List files in the directory to help debug
            directory = os.path.dirname(xsd_path)
            logging.debug(f"Files in directory '{directory}': {os.listdir(directory)}")

            if not os.path.exists(xsd_path):
                logging.error(f"XSD file not found: {xsd_path}")
                return

            is_valid, error_log = self.validate_xml(xml_path, xsd_path)
            if is_valid:
                logging.info("The XML file is valid.")
            else:
                logging.error("The XML file is invalid.")
                for error in error_log:
                    logging.error(error.message)

            # Generate and save sample XML data
            xml_data = self.generate_sample_xml()
            self.save_xml(xml_data, "oracle_output.xml")

            # Validate the generated XML data against the XSD schema
            generated_xml_path = os.path.abspath("oracle_output.xml")
            is_valid, error_log = self.validate_xml(generated_xml_path, xsd_path)
            if is_valid:
                logging.info("The generated XML file from Oracle data is valid.")
            else:
                logging.error("The generated XML file from Oracle data is invalid.")
                for error in error_log:
                    logging.error(error.message)

            Repository().ExportToTabDelimitedText() # optional
            logging.info('*** Stop ***')
        except Exception as e:
            logging.exception("Exception occurred")

    def Config(self):
        import shutil
        processed_dir = os.path.join(os.getcwd(), 'data', 'processed')
        if os.path.exists(processed_dir):
            shutil.rmtree(processed_dir)
        os.makedirs(processed_dir)

        import zipfile
        path_to_zip_file = os.path.join(os.getcwd(), 'data')
        files = [x for x in os.listdir(path_to_zip_file) if x.endswith(".zip")]
        for filename in files:
            with zipfile.ZipFile(os.path.join(path_to_zip_file, filename), 'r') as zip_ref:
                zip_ref.extractall(path_to_zip_file)
            os.rename(os.path.join(path_to_zip_file, filename), os.path.join(processed_dir, filename + ".unzipped"))

    def validate_xml(self, xml_path, xsd_path):
        with open(xsd_path, 'rb') as xsd_file:
            xsd_content = xsd_file.read()
        xsd_doc = etree.XML(xsd_content)
        xsd_schema = etree.XMLSchema(xsd_doc)

        with open(xml_path, 'rb') as xml_file:
            xml_content = xml_file.read()
        xml_doc = etree.XML(xml_content)

        is_valid = xsd_schema.validate(xml_doc)
        return is_valid, xsd_schema.error_log

    # def connect_to_oracle(self, username, password, dsn):
    #     connection = oracledb.connect(user=username, password=password, dsn=dsn)
    #     return connection

    # def query_oracle_and_generate_xml(self, connection, query):
    #     cursor = connection.cursor()
    #     cursor.execute(query)
    #     rows = cursor.fetchall()
    #     cursor.close()

    #     root = etree.Element("NaaccrData", xmlns="http://naaccr.org/naaccrxml")
    #     for row in rows:
    #         patient_element = etree.SubElement(root, "Patient")
    #         tumor_element = etree.SubElement(patient_element, "Tumor")
    #         for idx, value in enumerate(row):
    #             item_element = etree.SubElement(tumor_element, "Item", naaccrId=f"Field{idx+1}")
    #             item_element.text = str(value)

    #     xml_data = etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8")
    #     return xml_data

    def generate_sample_xml(self):
        root = etree.Element("NaaccrData", 
                             xmlns="http://naaccr.org/naaccrxml",
                             baseDictionaryUri="http://naaccr.org/naaccrxml/naaccr-dictionary-230.xml",
                             recordType="I",
                             specificationVersion="1.6")
        patient_element = etree.SubElement(root, "Patient")
        tumor_element = etree.SubElement(patient_element, "Tumor")
        item_element = etree.SubElement(tumor_element, "Item", naaccrId="Field1")
        item_element.text = "Sample Data"
        
        xml_data = etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8")
        return xml_data

    def save_xml(self, xml_data, filename):
        output_path = os.path.abspath(filename)
        with open(output_path, 'wb') as file:
            file.write(xml_data)
        logging.info(f"Saved XML to {output_path}")

if __name__ == "__main__":
    app = App()
    app.Process()
