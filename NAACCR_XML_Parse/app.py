import os
import logging
from utility import Utility
from handler.xmlLoadHandler import xmlLoadHandler
from repository import Repository
from lxml import etree
import oracledb

class App:
    def __init__(self):
        # Initialize utility and configure logging
        self._util = Utility()
        self._util.ConfigureLogging()

    def Process(self):
        try:
            logging.info('*** Start ***')
            self.Config()  # Configure directories and files
            Repository().Configure()  # Create and configure the local database

            handler = xmlLoadHandler()
            handler.Process()  # Process XML and CSV files to save data into the local database

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

            # Export data to tab-delimited text (optional)
            Repository().ExportToTabDelimitedText()
            logging.info('*** Stop ***')
        except Exception as e:
            logging.exception("Exception occurred")

    def Config(self):
        import shutil

        # Define the processed directory path
        processed_dir = os.path.join(os.getcwd(), 'data', 'processed')
        # Remove the processed directory if it exists and recreate it
        if os.path.exists(processed_dir):
            shutil.rmtree(processed_dir)
        os.makedirs(processed_dir)

        import zipfile
        path_to_zip_file = os.path.join(os.getcwd(), 'data')
        # Extract all zip files in the data directory
        files = [x for x in os.listdir(path_to_zip_file) if x.endswith(".zip")]
        for filename in files:
            with zipfile.ZipFile(os.path.join(path_to_zip_file, filename), 'r') as zip_ref:
                zip_ref.extractall(path_to_zip_file)
            os.rename(os.path.join(path_to_zip_file, filename), os.path.join(processed_dir, filename + ".unzipped"))

    def validate_xml(self, xml_path, xsd_path):
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
        return is_valid, xsd_schema.error_log

    def mock_oracle_data(self):
        # Simulate the data retrieved from Oracle with multiple tumors per patient
        return [
            {
                "patientIdNumber": "00100001",
                "birthplaceCountry": "ZZU",
                "sex": "2",
                "spanishHispanicOrigin": "9",
                "dateOfBirth": "19750102",
                "placeOfDeath": "997",
                "raceCodingSysCurrent": "6",
                "computedEthnicity": "0",
                "vitalStatus": "1",
                "race5": "99",
                "raceCodingSysOriginal": "9",
                "birthplace": "999",
                "race1": "99",
                "race2": "99",
                "race3": "99",
                "dateOfLastContact": "20240401",
                "race4": "99",
                "birthplaceState": "ZZ",
                "causeOfDeath": "0000",
                "icdRevisionNumber": "0",
                "tumors": [
                    {
                        "tumorRecordNumber": "01",
                        "primarySite": "C505",
                        "histologicTypeIcdO3": "8500",
                        "metsAtDxBrain": "0",
                        "lymphVascularInvasion": "1",
                        "ajccTnmClinN": "cN2a",
                        "regionalNodesPositive": "03",
                        "ajccTnmClinM": "cM0",
                        "siteCodingSysCurrent": "5",
                        "ajccTnmClinT": "cT4a",
                        "oncotypeDxRecurrenceScoreInvasiv": "XX9",
                        "morphCodingSysOriginl": "7",
                        "laterality": "2",
                        "primaryPayerAtDx": "10",
                        "rxSummHormone": "99",
                        "ambiguousTerminologyDx": "0",
                        "rxSummChemo": "99",
                        "addrAtDxState": "AB",
                        "icdO3ConversionFlag": "0",
                        "rxSummBrm": "99",
                        "gradeClinical": "3",
                        "her2OverallSummary": "0",
                        "siteCodingSysOriginal": "5",
                        "tnmEditionNumber": "08",
                        "ajccId": "48.2",
                        "typeOfReportingSource": "1",
                        "metsAtDxBone": "0",
                        "metsAtDxDistantLn": "0",
                        "schemaId": "00480",
                        "multiplicityCounter": "01",
                        "ajccTnmClinNSuffix": "(f)",
                        "ajccTnmClinStageGroup": "3C",
                        "addrAtDxPostalCode": "T0O0O0",
                        "metsAtDxLung": "0",
                        "countyAtDxGeocode2020": "998",
                        "countyAtDx": "000",
                        "ajccTnmPathStageGroup": "3A",
                        "estrogenReceptorSummary": "0",
                        "behaviorCodeIcdO3": "3",
                        "gradePathological": "3",
                        "reasonForNoSurgery": "1",
                        "multTumRptAsOnePrim": "88",
                        "ajccTnmPathT": "pT2",
                        "ageAtDiagnosis": "087",
                        "sequenceNumberCentral": "00",
                        "dateOfDiagnosis": "20210102",
                        "rxSummRadiation": "9",
                        "morphCodingSysCurrent": "7",
                        "regionalNodesExamined": "18",
                        "metsAtDxOther": "0",
                        "tumorSizeClinical": "040",
                        "rxSummSurgPrimSite": "99",
                        "followUpSourceCentral": "29",
                        "rxSummScopeRegLnSur": "9",
                        "followUpSource": "8",
                        "diagnosticConfirmation": "1",
                        "progesteroneRecepSummary": "0",
                        "tumorSizePathologic": "043",
                        "her2IshSummary": "8",
                        "ajccTnmPathM": "cM0",
                        "ajccTnmPathN": "pN1a",
                        "metsAtDxLiver": "0",
                        "rxSummOther": "9"
                    }
                ]
            }
        ]

    def generate_sample_xml(self):
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

        # Use the mock Oracle data to generate XML
        data = self.mock_oracle_data()

        # Parse the NAACCR dictionary to get patient and tumor item mappings
        dictionary_path = os.path.abspath(os.path.join(os.getcwd(), "NAACCR_XML_Parse", "naaccr-dictionary-230.xml"))
        logging.info(f"Dictionary path: {dictionary_path}")  # Add this line to check the dictionary path
        patient_items, tumor_items = self.parse_naaccr_dictionary(dictionary_path)

        for patient_data in data:
            # Create a Patient element
            patient_element = etree.SubElement(root, "Patient")

            # Adding Patient Items
            for naaccr_id, value in patient_data.items():
                if naaccr_id in patient_items:
                    item_element = etree.SubElement(patient_element, "Item", naaccrId=naaccr_id)
                    item_element.text = value

            # Adding Tumor elements
            for tumor_data in patient_data["tumors"]:
                tumor_element = etree.SubElement(patient_element, "Tumor")
                for naaccr_id, value in tumor_data.items():
                    if naaccr_id in tumor_items:
                        item_element = etree.SubElement(tumor_element, "Item", naaccrId=naaccr_id)
                        item_element.text = value

        # Convert the XML tree to a string
        xml_data = etree.tostring(root, pretty_print=True, xml_declaration=True, encoding="UTF-8")
        return xml_data

    def parse_naaccr_dictionary(self, dictionary_path):
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

    def save_xml(self, xml_data, filename):
        # Save the generated XML to a file
        output_path = os.path.abspath(filename)
        with open(output_path, 'wb') as file:
            file.write(xml_data)
        logging.info(f"Saved XML to {output_path}")

if __name__ == "__main__":
    # Run the application
    app = App()
    app.Process()
