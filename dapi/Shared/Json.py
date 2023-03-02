import dataclasses
import json

class EnhancedJSONEncoder (json.JSONEncoder):
    def default (self, object):
        if dataclasses.is_dataclass (object):
            return dataclasses.asdict (object)
        return super ().default (object)