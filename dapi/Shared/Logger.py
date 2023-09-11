import sys
from os.path import exists
import logging
import datetime 
from colorama import Fore

from .PrettyPrint import Pretty
from .BasicDecorators import execution_output

class Logger:
    _logLevel = None
    
    def __init__ (self) -> None:    
        if Logger._logLevel is not None: # Nothing to do
            return
        Logger.set_log_level()
    
    @staticmethod
    @execution_output
    def set_log_level(logLevel:str='debug') -> None:
        if Logger._logLevel is not None and Logger._logLevel == logLevel: # Nothing to do
            print (Pretty.assemble_simple ("Nothing to do, same log level."))
            return
        Logger._logLevel = logging.getLevelName (logLevel.upper())        
        logging.basicConfig (stream=sys.stderr, level=logging.DEBUG, format='%(message)s')

        Logger._logger = logging.getLogger()
        Logger._consoleHandler = logging.StreamHandler ()
        Logger._consoleHandler.setLevel (Logger._logLevel)
        Logger._formatter = logging.Formatter ('%(message)s')
        Logger._consoleHandler.setFormatter (Logger._formatter)

        Logger._logger.handlers.clear()
        Logger._logger.addHandler (Logger._consoleHandler)
        Logger._logger.setLevel (Logger._logLevel)

        Logger.debug = Logger._logger.debug
        Logger.warning = Logger._logger.warning
        Logger.info = Logger._logger.info
        Logger.error = Logger._logger.error
        Logger.critical = Logger._logger.critical
        print (Pretty.assemble (value=f"Log level set to {logLevel}", color=Fore.BLUE, tabCount=Pretty.Indent+1))
        return
