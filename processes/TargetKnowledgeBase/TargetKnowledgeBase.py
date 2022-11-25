from abc import ABC,abstractmethod

class TargetKnowledgeBase(ABC):

    @abstractmethod
    def publish (self, documentName, configParam):
        raise NotImplementedError