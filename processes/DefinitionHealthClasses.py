import copy
from dataclasses import dataclass, field

from SharedDataClasses import CountPercentage

def default_field(obj):
    return field(default_factory=lambda: copy.copy(obj))

@dataclass 
class StatsTotal:
    number_of_relations: int = 0
    overwritten_concepts: CountPercentage = None
    number_of_columns: int = 0
    ok_columns: CountPercentage = None
    errors: CountPercentage = None
    type_errors: CountPercentage = None
    documentation_errors: CountPercentage = None

@dataclass
class StatsRelation:
    schema_name: str
    relation_name: str
    number_of_columns: int = 0
    overwritten_concepts: CountPercentage = None
    ok_columns: CountPercentage = None
    errors: CountPercentage = None
    type_errors: CountPercentage = None
    documentation_errors: CountPercentage = None

@dataclass
class Stats:
    total: StatsTotal = StatsTotal()
    relation: list[StatsRelation] = default_field([])

@dataclass
class Concept:
    schema_name: str
    relation_name: str
    column_name: str
    concept_name: str

@dataclass
class Error:
    schema_name: str
    relation_name: str
    column_name: str
    message: str

@dataclass
class Errors:
    type: list[Error] = default_field([])
    documentation: list[Error] = default_field([])

@dataclass
class HealthReport: # Root 
    api_name: str
    stats: Stats = Stats()
    overwritten_concepts: list[Concept] = default_field([])
    errors: Errors = Errors()