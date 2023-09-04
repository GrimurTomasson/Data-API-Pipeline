import functools
import datetime 
import inspect

from time import monotonic

from .Audit import Audit

def get_parameter_info (function, values) -> str:
    keys = list (inspect.signature (function).parameters.keys())
    if len (keys) > 0 and keys[0] == 'self':
        keys = keys[1:]
        values = values[1:]
    retVal = str.join(', ', [f"{key}: {value}" for key, value in zip(keys, values)])
    return retVal
    

# Circular dependency ef við skiljum þetta eftir inni í Decorators
def audit (_func=None, *, tabCount=0):
    def decorator_audit (function):
        @functools.wraps (function)
        def wrapper (*args, **kwargs):
            startTime = monotonic ()
            startDatetime = datetime.datetime.now ()
            retval = function (*args, **kwargs)
            execution_time = datetime.timedelta (seconds = monotonic () - startTime).total_seconds ()
            params = get_parameter_info (function, args)    
            Audit.dapi (startDatetime, function.__qualname__, params, execution_time)
            return retval
        return wrapper
    if _func is None:
        return decorator_audit
    return decorator_audit (_func)

def audit_dbt (_func=None, *, tabCount=0):
    def decorator_audit (function):
        @functools.wraps (function)
        def wrapper (*args, **kwargs):
            startDatetime = datetime.datetime.now ()
            retval = function (*args, **kwargs)
            Audit.dbt (startDatetime, function.__qualname__)
            return retval
        return wrapper
    if _func is None:
        return decorator_audit
    return decorator_audit (_func)