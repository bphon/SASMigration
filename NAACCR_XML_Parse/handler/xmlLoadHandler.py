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
        self._data_dir = os.path.join(os.getcwd(), "SASMigration", "NAACCR_XML_Parse", "data")
        self._processed_dir = os.path.join(os.getcwd(), "SASMigration", "NAACCR_XML_Parse", "data", "processed")
        os.makedirs(self._processed_dir, exist_ok=True)
        self._patients = []
        self._fieldmapping = fieldMapping
        self._columnmapping = columnMapping
        self._xsd_path = os.path.join(os.getcwd(), "SASMigration", "NAACCR_XML_Parse", "naaccr_data_1.6.xsd")

    def Process(self):
        # Process XML files
        xml_files = glob.glob(os.path.join(self._data_dir, "*.XML"))
        xml_files.sort(key=os.path.getmtime)
        for filepath in xml_files:
            self._patients.clear()
            logging.info(f"[processing XML {filepath}]")
            tree = ET.parse(filepath)
            root = tree.getroot()
            hi = self.ParseHeaderFields(root)
            logging.info(f"- parsing XML")
            self.ParsePatients(root, hi)
            logging.info(f"- saving patients and tumors")
            self.SavePatients()
            self.MoveFile(filepath)
        
        # Process CSV files
        csv_files = glob.glob(os.path.join(self._data_dir, "*.csv"))
        csv_files.sort(key=os.path.getmtime)
        for filepath in csv_files:
            self._patients.clear()
            logging.info(f"[processing CSV {filepath}]")
            self.ParseCsv(filepath)
            xml_data = self.generate_xml()
            self.save_xml(xml_data, filepath)
            self.MoveFile(filepath)

            # Validate XML
            if self.validate_xml(xml_data):
                logging.info("XML Validation Successful")
            else:
                logging.error("XML Validation Failed")

    def ParseHeaderFields(self, root):
        hi = HeaderInfo()
        nodes = [x for x in root if x.tag == "{http://naaccr.org/naaccrxml}Item"]
        for node in nodes:
            setattr(hi, node.attrib.get("naaccrId"), node.text)
        return hi

    def ParsePatients(self, root, headerInfo):
        patients = [x for x in root if x.tag == "{http://naaccr.org/naaccrxml}Patient"]
        for patient in patients:
            patient_data = {node.attrib.get("naaccrId"): node.text.strip() if node.text else "" for node in patient if node.tag == "{http://naaccr.org/naaccrxml}Item"}
            tumors = [x for x in patient if x.tag == "{http://naaccr.org/naaccrxml}Tumor"]
            tumor_list = []
            for tumor in tumors:
                ti = TumorItem()
                for k, v in headerInfo.__dict__.items():
                    setattr(ti, k, v.strip() if v else "")
                for k, v in patient_data.items():
                    setattr(ti, k, v)
                nodes = [x for x in tumor if x.tag == "{http://naaccr.org/naaccrxml}Item"]
                for node in nodes:
                    setattr(ti, node.attrib.get("naaccrId"), node.text.strip() if node.text else "")
                tumor_list.append(ti)
            self._patients.append((patient_data, tumor_list))

    def ParseCsv(self, filepath):
        with open(filepath, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                ti = TumorItem()
                for field, column in self._fieldmapping.items():
                    value = row.get(column, '').strip()
                    setattr(ti, field, value)
                self._patients.append((None, [ti]))
        logging.info(f"Parsed {len(self._patients)} patients from CSV {filepath}")

    def generate_xml(self):
        root = ET.Element("NaaccrData", xmlns="http://naaccr.org/naaccrxml")
        
        for patient_data, tumors in self._patients:
            patient_element = ET.SubElement(root, "Patient")
            if patient_data:
                for field_id, value in patient_data.items():
                    item_element = ET.SubElement(patient_element, "Item", naaccrId=field_id)
                    item_element.text = value
            for tumor_data in tumors:
                tumor_element = ET.SubElement(patient_element, "Tumor")
                for field_id, value in vars(tumor_data).items():
                    item_element = ET.SubElement(tumor_element, "Item", naaccrId=field_id)
                    item_element.text = value

        logging.info("Generated XML content from data")
        return ET.tostring(root, encoding='utf-8', method='xml').decode('utf-8')

    def save_xml(self, xml_data, filepath):
        base_filename = os.path.basename(filepath)
        new_filename = os.path.splitext(base_filename)[0] + ".xml"
        output_path = os.path.join(self._processed_dir, new_filename)
        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(xml_data)
        logging.info(f"Saved XML to {output_path}")

    def validate_xml(self, xml_data):
        schema = etree.XMLSchema(file=self._xsd_path)
        xml_doc = etree.fromstring(xml_data.encode('utf-8'))
        return schema.validate(xml_doc)

    def SavePatients(self):
        qty = 0
        sql, keys = self.GenerateInsertSQL()
        conn = sqlite3.connect('naaccr_data.db')
        cursor = conn.cursor()
        for patient_data, tumors in self._patients:
            for tumor in tumors:
                if self.MostRecentTumor(conn, tumor):
                    values = []
                    for key in keys:
                        if hasattr(tumor, key):
                            attribute = getattr(tumor, key)
                            if not self.MaxLength(key, attribute):
                                attribute = None
                            values.append(attribute)
                        else:
                            values.append(None)
                    self.DeleteTumor(conn, tumor)
                    cursor.execute(sql, values)
                    conn.commit()
                    qty += 1
        logging.info(f"[number of XML/CSV items saved]: {qty}")

    def SaveTumors(self):
        # Implementation for saving tumor data if separate logic is required
        pass

    def MostRecentTumor(self, conn, tumor):
        params = (getattr(tumor,"medicalRecordNumber", getattr(tumor,"patientIdNumber", "")), 
            getattr(tumor, "tumorRecordNumber"), getattr(tumor, "registryId"), getattr(tumor, "dateCaseReportExported", getattr(tumor,"dateCaseLastChanged", "")))
        sql = """select * from NAACCR_DATA 
            where MEDICAL_RECORD_NUMBER_N2300 = ? and TUMOR_RECORD_NUMBER_N60 = ? and REGISTRY_ID_N40 = ?
            and DATE_CASE_REPORT_EXPORT_N2110 > ?"""
        cmd = conn.cursor()
        cmd.execute(sql, params)
        row = cmd.fetchone()
        return row is None

    def DeleteTumor(self, conn, tumor):
        params = (getattr(tumor,"medicalRecordNumber", getattr(tumor,"patientIdNumber", "")), getattr(tumor, "tumorRecordNumber"), getattr(tumor, "registryId"))
        sql = """delete from NAACCR_DATA 
            where MEDICAL_RECORD_NUMBER_N2300 = ? and TUMOR_RECORD_NUMBER_N60 = ? and REGISTRY_ID_N40 = ?"""
        cmd = conn.cursor()
        cmd.execute(sql, params)
        conn.commit()

    def GenerateInsertSQL(self):
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
        sql = f"insert into NAACCR_DATA ({fields1}) values ({fields2})"
        return (sql, keys)

    def MoveFile(self, filepath):
        if not os.path.exists(self._processed_dir):
            os.makedirs(self._processed_dir)
        os.rename(filepath, os.path.join(self._processed_dir, os.path.basename(filepath)))

    def MaxLength(self, key, attribute):
        dbfield = self._fieldmapping[key]
        maxlen = self._columnmapping[dbfield]
        if len(attribute) > maxlen:
            print(f"value too long - column: {key}, value: {attribute}, maxlen: {maxlen}")
            return False
        return True

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    handler = xmlLoadHandler()
    handler.Process()
