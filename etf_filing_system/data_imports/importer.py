from abc import abstractmethod, ABC

class DataImporter(ABC):

    @abstractmethod
    async def parsefile(self, file):
        pass

    @abstractmethod
    def normalize(self, df):
        pass

    @abstractmethod
    def import_to_db(self):
        pass
