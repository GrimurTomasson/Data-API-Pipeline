from ..Shared.Config import Config
from .ConceptGlossary import ConceptGlossary
from .ConceptGlossaryRvk import ConceptGlossaryRvk
from .ConceptGlossaryNone import ConceptGlossaryNone

class ConceptGlossaryFactory:
    _supportedGlossaries = ['Rvk', 'None']

    def __init__(self):
        self._glossary = self.__generate_glossary ()

    def get_concept_glossary (self) -> ConceptGlossary:
        return self._glossary

    def __generate_glossary (self) -> ConceptGlossary:
        self._glossaryName = Config['concept-glossary']['type']
        if len (self._glossaryName) and self._glossaryName not in ConceptGlossaryFactory._supportedGlossaries:
            print(f"Concept glossary in config is not support by pipeline. Config: {self._glossaryName}. Supported glossaries: {ConceptGlossaryFactory._supportedGlossaries}")
            raise
        
        if self._glossaryName == 'Rvk':    
            return ConceptGlossaryRvk ()
        elif self._glossaryName == 'None':
            return ConceptGlossaryNone ()