try:
    from .importer import DataImporter
except ImportError:
    from importer import DataImporter

class NPortImporter(DataImporter):
    
    def __init__(self):
        pass
    
    def parsefile(self, file):
        pass

    def normalize(self):
        pass
