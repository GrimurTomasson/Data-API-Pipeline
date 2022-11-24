import Decorators
import APISupport

class Latest:

    def __init__ (self) -> None:
        APISupport.initialize ()
        
    @Decorators.output_headers
    @Decorators.execution_time
    def refresh (self):
        """Running dbt to refresh models and data (Latest)"""
        dbtOperation = ["dbt", "run", "--full-refresh"] #  --fail-fast fjarlægt þar sem dbt rakti dependencies ekki nógu vel
        APISupport.run_operation (APISupport.workingDirectory, APISupport.latest_path, dbtOperation)
        return

    @Decorators.output_headers
    @Decorators.execution_time
    def run_tests (self):
        """Running dbt tests"""
        dbtOperation = ["dbt", "--log-format", "json",  "test"]
        output = APISupport.run_operation (APISupport.workingDirectory, APISupport.latest_path, dbtOperation, True)
        APISupport.print_v (f"Output for dbt test results: {APISupport.dbt_test_output_file_info.qualified_name}")
        APISupport.write_file (output.stdout, APISupport.dbt_test_output_file_info.qualified_name)
        return