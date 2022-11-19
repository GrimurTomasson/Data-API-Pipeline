from dataclasses import dataclass
from SharedDataClasses import CountPercentage

@dataclass
class HeaderExecution:
    timestamp: str
    id: str

@dataclass 
class Header:
    api_name: str
    dbt_version: str
    execution: HeaderExecution

@dataclass
class StatsTotal:
    error: CountPercentage
    ok: CountPercentage
    skipped: CountPercentage
    total: CountPercentage

@dataclass
class RelationStats:
    name: str
    ok: CountPercentage
    error: CountPercentage
    total: int

@dataclass
class Stats:
    total: StatsTotal
    relation: list[RelationStats]

@dataclass
class Error:
    name: str
    rows_on_error: int
    sql_filename: str
    relation_name: str
    rows_in_relation: int
    rows_on_error_percentage: int
    query_path: str
    sql: str

@dataclass
class HealthReport:
    header: Header
    stats: Stats
    errors: list[Error]