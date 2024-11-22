import os
import json

from .ConceptGlossary import ConceptGlossary, ConceptGlossaryDefinition

from ..Shared.PrettyPrint import Pretty
from ..Shared.Logger import Logger
from ..Shared.Config import Config
from ..Shared.Decorators import post_execution_output


class ConceptGlossaryJson (ConceptGlossary):    
    filename = "concept_glossary.json" # Setja í config?
    
    def get_glossary_column_data (self, schemaName, tableName, columnName) -> ConceptGlossaryDefinition:
        if not hasattr (ConceptGlossaryJson, '_glossary'):
            self.load_glossary_data ()
        return ConceptGlossaryJson._glossary[columnName] 

    @post_execution_output
    def load_glossary_data (self):
        ConceptGlossaryJson._glossary = {}

        relative_path = Config['concept-glossary']['relative-path'] if 'concept-glossary' in Config._config and 'relative-path' in Config['concept-glossary'] else ''
        concept_glossary_filename =  os.path.join (Config.workingDirectory, relative_path, ConceptGlossaryJson.filename) 
        Logger.debug (Pretty.assemble_simple (f"Concept glossary file: {concept_glossary_filename}"))

        with open (concept_glossary_filename, encoding="utf-8") as json_file:
            enrichedCatalogJson = json.load(json_file)
            for concept in enrichedCatalogJson: # Get rid of the header
                max_len = int (concept['max_length'] if 'max_length' in concept else -1)
                ConceptGlossaryJson._glossary[concept['column_name']] = ConceptGlossaryDefinition (concept['column_name'], concept['description'], concept['data_type'], max_len)
                
        Logger.debug (Pretty.assemble_simple (f"Number of concept glossary entries: {len (ConceptGlossaryJson._glossary)}"))
        return
    
# [
# 	{
# 		"column_name": "id"
# 		,"description": "Einkvæmt auðkenni"
# 		,"data_type": "nvarchar"
# 		,"max_length": "250"
# 	},
# 	{
# 		"column_name": "kennitala"
# 		,"description": "Kennitala Þjóðskrár"
# 		,"data_type": "nvarchar"
# 		,"max_length": "10"
# 	}
# ]