from abc import abstractmethod, ABC

class DataImporter(ABC):

    @abstractmethod
    def parsefile(self, file):
        pass

    @abstractmethod
    def normalize(self):
        pass

