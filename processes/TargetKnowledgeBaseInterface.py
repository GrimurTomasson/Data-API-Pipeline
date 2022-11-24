from abc import ABC,abstractmethod

class TargetKnowledgeBaseInterface(ABC):

    @abstractmethod
    def publish (self, documentName, configParam):
        raise NotImplementedError