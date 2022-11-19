import copy
from dataclasses import dataclass, field

from SharedDataClasses import CountPercentage

def default_field(obj):
    return field(default_factory=lambda: copy.copy(obj))

@dataclass
class ColumnType:
    name: str = ''
    length: str = ''

@dataclass
class ColumnDescription:
    text: str = ''
    origin: str = ''
    missing: bool = False

@dataclass
class Column:
    name: str = ''
    type: ColumnType = ColumnType()
    description: ColumnDescription = ColumnDescription()

@dataclass 
class Relation:
        schema_name: str = ''
        relation_name: str = ''
        columns: list[Column] = default_field([])

@dataclass
class Documentation:
    relations: list[Relation] = default_field([])