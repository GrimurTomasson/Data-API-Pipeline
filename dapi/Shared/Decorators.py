import functools
from time import gmtime, strftime, monotonic
import datetime 
from colorama import Fore

from .Logger import Logger
from .PrettyPrint import Pretty

def output_headers (_func=None, *, tabCount=0):
    def decorator_output_headers (function):
        @functools.wraps (function)
        def wrapper (*args, **kwargs):
            Logger.info (Pretty.assemble (f"STARTING - {function.__doc__} ({function.__qualname__})", True, True, Fore.GREEN, 0, tabCount))
            retval = function (*args, **kwargs)
            Logger.info (Pretty.assemble (f"FINISHING - {function.__doc__} ({function.__qualname__})", False, True, Fore.GREEN, 0, tabCount))
            return retval
        return wrapper
    if _func is None:
        return decorator_output_headers
    return decorator_output_headers (_func)

def execution_time (_func=None, *, tabCount=0):
    def decorator_execution_time (function):
        @functools.wraps (function)
        def wrapper (*args, **kwargs):
            startTime = monotonic ()
            retval = function (*args, **kwargs)
            execution_time = datetime.timedelta (seconds = monotonic () - startTime).total_seconds ()
            Logger.info (Pretty.assemble (f"\n\tExecution time in seconds: {execution_time} - ({function.__qualname__})", False, False, Fore.LIGHTBLUE_EX, 0, tabCount + 1))
            return retval
        return wrapper
    if _func is None:
        return decorator_execution_time
    return decorator_execution_time (_func)
            
            
            