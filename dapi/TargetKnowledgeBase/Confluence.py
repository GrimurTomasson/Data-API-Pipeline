import os

from ..Shared.Environment import Environment
from ..Shared.Config import Config
from ..Shared.Utils import Utils
from .TargetKnowledgeBase import TargetKnowledgeBase

class Confluence (TargetKnowledgeBase):
    def __init__ (self) -> None:
        return

    def publish (self, documentName, configParam):
        if Config['documentation']['publish-to'] != 'Confluence' or Config['documentation'][configParam]['publish'] != True:
            print (f"{configParam} not published due to config settings!")
            return
        
        if self.retrieve_env_variables () is True:
            self.public_user_pass (documentName)
        else:
            self.publish_config (documentName)
        return

    def retrieve_env_variables (self) -> bool:
        found = 0
        if os.environ.get(Environment.markUser) is not None and len (os.environ.get(Environment.markUser)) > 0:
            self._user = os.environ.get(Environment.markUser)
            found += 1
        
        if os.environ.get(Environment.markPassword) is not None and len (os.environ.get(Environment.markPassword)) > 0:
            self._pwd = os.environ.get(Environment.markPassword)
            found += 1

        if os.environ.get(Environment.markBaseUri) is not None and len (os.environ.get(Environment.markBaseUri)) > 0:
            self._uri = os.environ.get(Environment.markBaseUri)
            found += 1
        return found == 3

    def publish_config (self, documentName):
        qualifiedConfigName = os.path.join (Config.workingDirectory, "mark_config.txt")
        # Skoða að setja trace flaggið inn líka, þegar verbose er sett í config!
        # Útfært með: https://github.com/kovetskiy/mark
        operation = ['mark', '-c', qualifiedConfigName, '-f', documentName]
        Utils.run_operation (Config.workingDirectory, Config.workingDirectory, operation)
        return
    
    def public_user_pass (self, documentName):
        operation = ['mark', '-u', self._user, '-p', self._pwd, '-b', self._uri, '-f', documentName]
        Utils.run_operation (Config.workingDirectory, Config.workingDirectory, operation)
        return