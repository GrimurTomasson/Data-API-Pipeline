import APISupport
from TargetKnowledgeBaseInterface import TargetKnowledgeBaseInterface

class TargetKnowledgeBase_Confluence (TargetKnowledgeBaseInterface):

    def publish (self, documentName, configParam):
        if APISupport.config['documentation']['publish-to'] != 'Confluence' or APISupport.config['documentation']['publish'][configParam] != True:
            print (f"{configParam} not published due to config settings!")
            return
        
        qualifiedConfigName = f"{APISupport.workingDirectory}/mark_config.txt"
        # Skoða að setja trace flaggið inn líka, þegar verbose er sett í config!
        operation = ['mark', '-c', qualifiedConfigName, '-f', documentName]
        APISupport.run_operation (APISupport.workingDirectory, APISupport.workingDirectory, operation)
        return
