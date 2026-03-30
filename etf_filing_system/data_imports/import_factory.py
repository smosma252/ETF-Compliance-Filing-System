
from .fund_info_importer import FundInfoImporter
from .n_port_importer import NPortImporter

class ImportFactory:

    @staticmethod
    def get_importer(fund_type):
        if fund_type == "fund_info":
            return FundInfoImporter()
        elif fund_type == "n_port":
            return NPortImporter()
        else:
            raise Exception("Invalid FundType")
        
        

