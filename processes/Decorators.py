import functools
import time
import datetime 

def output_headers (_func=None, *, tabCount=0):
    def decorator_output_headers (function):
        @functools.wraps (function)
        def wrapper (*args, **kwargs):
            separator = "-" * 120
            tabs = "\t" * tabCount
            print (f"\n{tabs}{separator}\n\n{tabs}{function.__doc__} ({function.__qualname__})\n{tabs}{separator}\n")
            retval = function(*args, **kwargs)
            print (f"{tabs}{separator}\n")
            return retval
        return wrapper
    if _func is None:
        return decorator_output_headers
    return decorator_output_headers (_func)

def execution_time (_func=None, *, tabCount=0):
    def decorator_execution_time (function):
        @functools.wraps (function)
        def wrapper (*args, **kwargs):
            startTime = time.monotonic()
            retval = function(*args, **kwargs)
            execution_time = datetime.timedelta(seconds=time.monotonic() - startTime).total_seconds()
            tabs = "\t" * tabCount
            print (f"\n{tabs}{function.__qualname__} - Execution time in seconds: {execution_time}")
            return retval
        return wrapper
    if _func is None:
        return decorator_execution_time
    return decorator_execution_time (_func)