import os, logging
from utility import Utility
from handler.xmlLoadHandler import xmlLoadHandler
from repository import Repository
#from temp import Temp

# input data files 
# - place .xml files (or .zip containing .xml) in in $cwd\data
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
            Repository().ExportToTabDelimitedText() # optional
            logging.info('*** Stop ***')
        except:
            logging.exception("")

    def Config(self):
        # make sure processed directory exists and is ready
        import shutil
        processed_dir = os.path.join(os.getcwd(), 'data', 'processed')
        if os.path.exists(processed_dir):
            shutil.rmtree(processed_dir)
        os.makedirs(processed_dir)

        # unzip any zipped files
        import zipfile
        path_to_zip_file = os.path.join(os.getcwd(), 'data')
        files = [x for x in os.listdir(path_to_zip_file) if x.endswith(".zip")]
        for filename in files:
            with zipfile.ZipFile(os.path.join(path_to_zip_file, filename), 'r') as zip_ref:
                zip_ref.extractall(path_to_zip_file)
            os.rename(os.path.join(path_to_zip_file, filename), os.path.join(processed_dir, filename + ".unzipped"))

if __name__ == "__main__":
    app = App()
    app.Process()