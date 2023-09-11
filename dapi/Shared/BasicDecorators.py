import functools
from time import monotonic
import datetime 
from colorama import Fore

from .PrettyPrint import Pretty

def execution_output (_func=None): 
    def decorator_output (function):
        @functools.wraps (function)
        def wrapper (*args, **kwargs):
            startTime = monotonic ()
            printableStartTime = datetime.datetime.now ().strftime ("%H:%M:%S")
            retval = None
            status = "OK"
            thrownException = None
            print (Pretty.assemble_output_start_message (printableStartTime, function.__qualname__))
            Pretty.add_indent ()
            
            try:
                retval = function (*args, **kwargs)
            except Exception as ex:
                status = "ERROR"
                thrownException = ex
            
            execution_time = datetime.timedelta (seconds = monotonic () - startTime).total_seconds ()
            printableEndTime = datetime.datetime.now ().strftime ("%H:%M:%S")
            Pretty.reduce_indent ()
            print (Pretty.assemble_output_end_message (printableStartTime, function.__qualname__, status, printableEndTime, execution_time))
            
            if thrownException is not None:
                raise (thrownException)
            return retval
        return wrapper
    if _func is None:
        return decorator_output
    return decorator_output (_func)