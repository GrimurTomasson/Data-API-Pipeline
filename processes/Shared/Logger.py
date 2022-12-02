import sys
import logging

from Shared.ConfigBase import ConfigBase

class Logger:
    logLevel = ConfigBase.process_config()['log-level']
    _logLevel = logging.getLevelName (logLevel.upper())        

    logging.basicConfig (stream=sys.stderr, level=logging.DEBUG, format='%(message)s')

    _logger = logging.getLogger()
    _consoleHandler = logging.StreamHandler ()
    _consoleHandler.setLevel (_logLevel)
    _formatter = logging.Formatter ('%(message)s')
    _consoleHandler.setFormatter (_formatter)

    _logger.handlers.clear()
    _logger.addHandler (_consoleHandler)
    _logger.setLevel (_logLevel)


    debug = _logger.debug
    warning = _logger.warning
    info = _logger.info
    error = _logger.error
    critical = _logger.critical
