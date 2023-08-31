import functools
import datetime 

from time import monotonic

from .Audit import Audit

# Circular dependency ef við skiljum þetta eftir inni í Decorators
def audit (_func=None, *, tabCount=0):
    def decorator_audit (function):
        @functools.wraps (function)
        def wrapper (*args, **kwargs):
            startTime = monotonic ()
            startDatetime = datetime.datetime.now ()
            retval = function (*args, **kwargs)
            execution_time = datetime.timedelta (seconds = monotonic () - startTime).total_seconds ()
            Audit.dapi (startDatetime, function.__qualname__, execution_time)
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