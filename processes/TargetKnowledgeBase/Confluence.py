from Shared.Config import Config
from Shared.Utils import Utils
from TargetKnowledgeBase.TargetKnowledgeBase import TargetKnowledgeBase

class Confluence (TargetKnowledgeBase):
    def __init__ (self) -> None:
        self._config = Config ()
        self._utils = Utils ()

    def publish (self, documentName, configParam):
        if self._config['documentation']['publish-to'] != 'Confluence' or self._config['documentation']['publish'][configParam] != True:
            print (f"{configParam} not published due to config settings!")
            return
        
        qualifiedConfigName = f"{self._config.workingDirectory}/mark_config.txt"
        # Skoða að setja trace flaggið inn líka, þegar verbose er sett í config!
        operation = ['mark', '-c', qualifiedConfigName, '-f', documentName]
        self._utils.run_operation (self._config.workingDirectory, self._config.workingDirectory, operation)
        return
