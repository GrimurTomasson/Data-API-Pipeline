from .ConceptGlossary import ConceptGlossary, ConceptGlossaryDefinition

class ConceptGlossaryNone (ConceptGlossary):
    def get_glossary_column_data (self, schemaName, tableName, columnName) -> ConceptGlossaryDefinition:
        return ConceptGlossaryDefinition('', '', '', '')
