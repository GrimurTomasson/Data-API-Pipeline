from Shared.Decorators import output_headers, execution_time
from Shared.Config import Config
from Shared.Utils import Utils

class Latest:
    def __init__ (self) -> None:
        self._config = Config ()
        self._utils = Utils ()
        
    @output_headers
    @execution_time
    def refresh (self):
        """Running dbt to refresh models and data (Latest)"""
        dbtOperation = ["dbt", "run", "--full-refresh"] #  --fail-fast fjarlægt þar sem dbt rakti dependencies ekki nógu vel

        self._utils.print_v (f"working directory: {self._config.workingDirectory}")
        self._utils.print_v (f"Latest directory: {self._config.latestPath}")

        self._utils.run_operation (self._config.workingDirectory, self._config.latestPath, dbtOperation)
        return

    @output_headers
    @execution_time
    def run_tests (self):
        """Running dbt tests"""
        dbtOperation = ["dbt", "--log-format", "json",  "test"]
        #output = self._utils.run_operation (self._config.workingDirectory, self._config.latestPath, dbtOperation, True)
        self._utils.print_v (f"Output for dbt test results: {self._config.dbtTestOutputFileInfo.qualified_name}")
        #self._utils.write_file (output.stdout, self._config.dbtTestOutputFileInfo.qualified_name)
        return