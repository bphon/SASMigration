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
            Repository().Configure()  # create local db
            handler = xmlLoadHandler()
            handler.Process()  # read CSV files and save them to local db

            logging.info("Generating sample XML data...")
            # Generate and save sample XML data based on mock Oracle data
            xml_data = self.generate_sample_xml()
            self.save_xml(xml_data, "oracle_output.xml")

            # Validate the generated XML data against the XSD schema
            xsd_path = os.path.abspath(os.path.join(os.getcwd(), "NAACCR_XML_Parse", "naaccr_data_1.6.xsd"))
            generated_xml_path = os.path.abspath("oracle_output.xml")

            logging.info("Validating generated XML data...")
            is_valid, error_log = self.validate_xml(generated_xml_path, xsd_path)
            if is_valid:
                logging.info("The generated XML file from Oracle data is valid.")
            else:
                logging.error("The generated XML file from Oracle data is invalid.")
                for error in error_log:
                    logging.error(error)

            Repository().ExportToTabDelimitedText()  # optional
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

    def mock_oracle_data(self):
        # Simulate the data retrieved from Oracle
        return [
            {
                "patientIdNumber": "123456",
                "medicalRecordNumber": "MRN001",
                "tumorRecordNumber": "TRN001",
                "primarySite": "C509",
                "histology": "8500/3"
            },
            {
                "patientIdNumber": "123457",
                "medicalRecordNumber": "MRN002",
                "tumorRecordNumber": "TRN002",
                "primarySite": "C504",
                "histology": "8520/3"
            }
        ]

    def generate_sample_xml(self):
        root = etree.Element("NaaccrData",
                             xmlns="http://naaccr.org/naaccrxml",
                             baseDictionaryUri="http://naaccr.org/naaccrxml/naaccr-dictionary-230.xml",
                             recordType="I",
                             specificationVersion="1.6")

        # Use the mock Oracle data to generate XML
        data = self.mock_oracle_data()

        dictionary_path = os.path.abspath(os.path.join(os.getcwd(), "NAACCR_XML_Parse", "naaccr-dictionary-230.xml"))
        patient_items, tumor_items = self.parse_naaccr_dictionary(dictionary_path)

        for patient_data in data:
            patient_element = etree.SubElement(root, "Patient")
            
            # Adding Patient Items
            for naaccr_id, value in patient_data.items():
                if naaccr_id in patient_items:
                    item_element = etree.SubElement(patient_element, "Item", naaccrId=naaccr_id)
                    item_element.text = value
            
            tumor_element = etree.SubElement(patient_element, "Tumor")

            # Adding Tumor Items
            for naaccr_id, value in patient_data.items():
                if naaccr_id in tumor_items:
                    item_element = etree.SubElement(tumor_element, "Item", naaccrId=naaccr_id)
                    item_element.text = value

        xml_data = etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8")
        return xml_data
    
    def parse_naaccr_dictionary(self, dictionary_path):
        tree = etree.parse(dictionary_path)
        root = tree.getroot()
        patient_items = set()
        tumor_items = set()

        for item in root.xpath("//ns:ItemDef", namespaces={'ns': 'http://naaccr.org/naaccrxml'}):
            naaccr_id = item.get("naaccrId")
            parent_tag = item.get("parentXmlElement")
            if parent_tag == "Patient":
                patient_items.add(naaccr_id)
            elif parent_tag == "Tumor":
                tumor_items.add(naaccr_id)
        
        return patient_items, tumor_items

    def save_xml(self, xml_data, filename):
        output_path = os.path.abspath(filename)
        with open(output_path, 'wb') as file:
            file.write(xml_data)
        logging.info(f"Saved XML to {output_path}")

if __name__ == "__main__":
    app = App()
    app.Process()
