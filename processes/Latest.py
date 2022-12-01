from Shared.Decorators import output_headers, execution_time
from Shared.Config import Config
from Shared.Utils import Utils

class Latest:
    def __init__ (self) -> None:
        return
        
    @output_headers
    @execution_time
    def refresh (self):
        """Running dbt to refresh models and data (Latest)"""
        dbtOperation = ["dbt", "run", "--full-refresh"] #  --fail-fast fjarlægt þar sem dbt rakti dependencies ekki nógu vel
        Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation)
        return

    @output_headers
    @execution_time
    def run_tests (self):
        """Running dbt tests"""
        dbtOperation = ["dbt", "--log-format", "json",  "test"]
        output = Utils.run_operation (Config.workingDirectory, Config.latestPath, dbtOperation, True)
        Utils.print_v (f"\tOutput for dbt test results: {Config.dbtTestOutputFileInfo.qualified_name}")
        Utils.write_file (output.stdout, Config.dbtTestOutputFileInfo.qualified_name)
        return