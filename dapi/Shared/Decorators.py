import functools
from time import monotonic
import datetime 
from colorama import Fore

from .LogLevel import LogLevel
from .Logger import Logger
from .PrettyPrint import Pretty

def post_execution_output (_func=None, *, logLevel:LogLevel=LogLevel.DEBUG): 
    def decorator_output (function):
        @functools.wraps (function)
        def wrapper (*args, **kwargs):
            startTime = monotonic ()
            printableStartTime = datetime.datetime.now ().strftime ("%H:%M:%S")
            retval = None
            status = "OK"
            thrownException = None
            
            start_message = Pretty.assemble_output_start_message (printableStartTime, function.__qualname__)
            __log (start_message, logLevel)
            Pretty.add_indent ()
            
            try:
                retval = function (*args, **kwargs)
            except Exception as ex:
                status = "ERROR"
                thrownException = ex
            execution_time = datetime.timedelta (seconds = monotonic () - startTime).total_seconds ()
            printableEndTime = datetime.datetime.now ().strftime ("%H:%M:%S")
            
            Pretty.reduce_indent ()
            end_message = Pretty.assemble_output_end_message (printableStartTime, function.__qualname__, status, printableEndTime, execution_time)
            __log (end_message, logLevel)
                        
            if thrownException is not None:
                raise (thrownException)
            return retval
        return wrapper
    if _func is None:
        return decorator_output
    return decorator_output (_func)

def __log (message:str, logLevel:LogLevel) -> None:
    if logLevel == LogLevel.CRITICAL:
        Logger.critical (message)
    elif logLevel == LogLevel.ERROR:
        Logger.error (message)
    elif logLevel == LogLevel.WARNING:
        Logger.warning (message)
    elif logLevel == LogLevel.INFO:
        Logger.info (message)
    else:
        Logger.debug (message)
            
            