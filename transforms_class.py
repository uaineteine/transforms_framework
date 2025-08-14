import json
from pyspark.sql import DataFrame
from pipeline_event import PipelineEvent
from metaframe import Metaframe

class Transform(PipelineEvent):
    def __init__(self, name: str, description: str, transform_type:str):
        super().__init__(event_type="transform", message=name, description=description, log_location="events_log/job_1/transforms.json")
        self.name = name  # Set name manually
        self.transform_type = transform_type
    
    def transforms(self, df:DataFrame, df2:DataFrame=None):
        raise NotImplementedError("Subclasses should implement this method.")
    
    def __call__(self, tbl:Metaframe, tbl2:Metaframe=None):
        """
        Call the transformation function with the provided DataFrame(s).

        :param tbl: Primary Table / Spark DataFrame.
        :param tbl2: Optional Table / Spark DataFrame.
        :return: Transformed DataFrame.
        """
        return self.apply(tbl, tbl2)
    
    def apply(self, tbl:Metaframe, tbl2:Metaframe=None):
        #Apply transformation
        result_df = self.transforms(tbl, tbl2 = tbl2)

        self.log()

        return result_df

class TableTransform(Transform):
    def __init__(self, name: str, description: str, acts_on_variables: list[str]):
        super().__init__(name, description, "TableTransform")

        self.target_tables = [] #nadah to begin with

        self.target_variables = acts_on_variables
        if len(acts_on_variables) == 0:
            raise ValueError("No target variables defined for this transform.")
        
        #initalise variable lists
        self.created_variables = None
        self.renamed_variables = None
        self.deleted_variables = None

    
    def nvars(self):
        """
        Get the number of target variables for this transform.

        :return: Number of target variables.
        """
        return len(self.target_variables)
    
    def var(self):
        """
        Get the target variable for this transform.

        :return: The target variable.
        """
    
        if self.nvars() > 1:
            return self.target_variables
        elif self.nvars() == 1:
            return self.target_variables[0]
        else:
            raise ValueError("No target variables defined for this transform.")

class SimpleTransform(TableTransform):
    def __init__(self, name: str, description: str, acts_on_variable: str):
        super().__init__(name, description, [acts_on_variable])

class DropVariable(SimpleTransform):
    def __init__(self, variable_to_drop: str):
        #REPLACE HERE WITH YOUR OWN MESSAGE
        super().__init__("DropVariable", "Removes this variable from a dataframe", variable_to_drop)

    def transforms(self, tbl: Metaframe, tbl2: Metaframe = None):
        #PUT HERE ERROR CHECKING
        if self.var() not in tbl.df.columns:
            raise ValueError(f"Variable '{self.var()}' not found in DataFrame columns: {tbl.df.columns}")

        #PUT HERE TRANSFORMATION LOGIC
        self.deleted_variables = [self.var()]
        self.target_table = tbl.table_name
        tbl.df = tbl.df.drop(self.var())

        tbl.events.append(self)
        return tbl
