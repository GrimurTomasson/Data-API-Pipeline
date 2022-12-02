from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class ConceptGlossaryDefinition():
    name: str
    description: str
    data_type: str
    max_length: int

    def as_dictionary (self) -> dict[str, str]:
        return {'name':self.name, 'description':self.description, 'data_type':self.data_type, 'max_length':self.max_length}

class ConceptGlossary (ABC):
    
    @abstractmethod
    def get_glossary_column_data (self, schemaName, tableName, columnName) -> ConceptGlossaryDefinition:
        raise NotImplementedError