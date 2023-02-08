import os

from Shared.Config import Config
from Shared.Utils import Utils
from TargetKnowledgeBase.TargetKnowledgeBase import TargetKnowledgeBase

class Confluence (TargetKnowledgeBase):
    def __init__ (self) -> None:
        return

    def publish (self, documentName, configParam):
        if Config['documentation']['publish-to'] != 'Confluence' or Config['documentation'][configParam]['publish'] != True:
            print (f"{configParam} not published due to config settings!")
            return
        
        qualifiedConfigName = os.path.join (Config.workingDirectory, "mark_config.txt")
        # Skoða að setja trace flaggið inn líka, þegar verbose er sett í config!
        operation = ['mark', '-c', qualifiedConfigName, '-f', documentName]
        Utils.run_operation (Config.workingDirectory, Config.workingDirectory, operation)
        return
