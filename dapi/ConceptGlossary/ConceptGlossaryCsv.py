import os
import csv
from enum import IntEnum

from .ConceptGlossary import ConceptGlossary, ConceptGlossaryDefinition

from ..Shared.PrettyPrint import Pretty
from ..Shared.Logger import Logger
from ..Shared.Config import Config
from ..Shared.Decorators import post_execution_output

class CsvColumn (IntEnum):
    COLUMN_NAME = 0
    NAME = 1
    DESCRIPTION = 2
    DATA_TYPE = 3
    MAX_LENGTH = 4

class ConceptGlossaryCsv (ConceptGlossary):
    
    
    def get_glossary_column_data (self, schemaName, tableName, columnName) -> ConceptGlossaryDefinition:
        if not hasattr (ConceptGlossaryCsv, '_glossary'):
            self.load_glossary_data ()
        return ConceptGlossaryCsv._glossary[columnName] 

    @post_execution_output
    def load_glossary_data (self):
        ConceptGlossaryCsv._glossary = {}
        concept_glossary_filename =  os.path.join (Config.workingDirectory, "concept_glossary.csv")
        Logger.debug (Pretty.assemble_simple (f"Concept glossary file: {concept_glossary_filename}"))
        with open (concept_glossary_filename, newline='', encoding="utf-8") as cg_file:
            reader = csv.reader (cg_file, dialect='excel', quotechar='"') #delimiter=',', quotechar='"', )
            for row in reader: # Get rid of the header
                if row[0] == 'column_name': # Header
                    continue
                max_len = int (row[CsvColumn.MAX_LENGTH] if len (row[CsvColumn.MAX_LENGTH]) > 0 else -1)
                #Logger.debug (Pretty.assemble_simple (f"column_name: {row[CsvColumns.COLUMN_NAME]} - name: {row[CsvColumns.NAME]}"))
                ConceptGlossaryCsv._glossary[row[CsvColumn.COLUMN_NAME]] = ConceptGlossaryDefinition (row[CsvColumn.NAME], row[CsvColumn.DESCRIPTION], row[CsvColumn.DATA_TYPE], max_len)
                
        Logger.debug (Pretty.assemble_simple (f"Number of concept glossary entries: {len (ConceptGlossaryCsv._glossary)}"))
        return
