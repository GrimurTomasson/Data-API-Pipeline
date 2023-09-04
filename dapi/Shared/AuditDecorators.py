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
            retval = None
            status = 'success'
            thrownException = None
            stackDepth = len(inspect.stack(0))
            op_no_starts = Audit.get_and_increment_dapi_op_number_starts ()
            try:
                retval = function (*args, **kwargs)
            except Exception as ex:
                status = 'exception'
                thrownException = ex
            execution_time = datetime.timedelta (seconds = monotonic () - startTime).total_seconds ()
            params = get_parameter_info (function, args)    
            Audit.dapi (startDatetime, function.__qualname__, params, status, execution_time, stackDepth, op_no_starts)
            if thrownException is not None:
                raise (thrownException)
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
            retval = None
            status = 'success'
            thrownException = None
            try:
                retval = function (*args, **kwargs)
            except Exception as ex:
                status = 'exception'
                thrownException = ex
            Audit.dbt (startDatetime, function.__qualname__, status)
            if thrownException is not None:
                raise (thrownException)
            return retval
        return wrapper
    if _func is None:
        return decorator_audit
    return decorator_audit (_func)