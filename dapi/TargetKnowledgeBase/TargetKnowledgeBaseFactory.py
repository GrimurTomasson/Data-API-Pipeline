from ..Shared.Config import Config
from .TargetKnowledgeBase import TargetKnowledgeBase
from .Confluence import Confluence

class TargetKnowledgeBaseFactory:
    _supportedKnowledgebases = ['Confluence']
    
    def __init__(self):
        self._targetKnowledgeBase = self.__generate_target_knowledge_base ()

    def get_target_knowledge_base (self) -> TargetKnowledgeBase:
        return self._targetKnowledgeBase

    def __generate_target_knowledge_base (self) -> TargetKnowledgeBase:
        publishTo = Config['documentation']['publish-to']
        if len (publishTo) > 0 and publishTo not in TargetKnowledgeBaseFactory._supportedKnowledgebases:
            print (f"Documentation target is not supported, value: {publishTo} - Supported values: {TargetKnowledgeBaseFactory._supportedKnowledgebases}")
            raise
        
        if publishTo == 'Confluence':
            return Confluence ()