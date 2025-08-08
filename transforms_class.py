import json
from pyspark.sql import DataFrame
from metaplus_table import MetaplusTable, PipelineEvent

class Transform(PipelineEvent):
    def __init__(self, name: str, description: str):
        super().__init__(event_type="transform", name=name, description=description)
        self.created_variables = None
        self.renamed_variables = None
        self.deleted_variables = None
    
    def transforms(self, df:DataFrame, df2:DataFrame=None):
        raise NotImplementedError("Subclasses should implement this method.")
    
    def __call__(self, tbl:MetaplusTable, tbl2:MetaplusTable=None):
        """
        Call the transformation function with the provided DataFrame(s).

        :param tbl: Primary Table / Spark DataFrame.
        :param tbl2: Optional Table / Spark DataFrame.
        :return: Transformed DataFrame.
        """
        return self.apply(tbl, tbl2)
    
    def apply(self, tbl:MetaplusTable, tbl2:MetaplusTable=None):
        #Apply transformation
        result_df = self.transforms(tbl, tbl2 = tbl2)

        self.dump_to_json()

        return result_df

    def dump_to_json(self):
        # Auto-dump all instance variables to JSON
        with open(f"{self.name}_transform.json", "w") as f_out:
            json.dump(self.__dict__, f_out, indent=4)

class VariableTransform(Transform):
    def __init__(self, name: str, description: str, acts_on_variable: str):
        super().__init__(name, description)
        self.target_table = "Uncalled"
        self.target_variable = acts_on_variable

class DropVariable(VariableTransform):
    def __init__(self, variable_to_drop: str):
        super().__init__("DropVariable", "Removes this variable from a dataframe", variable_to_drop)

    def transforms(self, tbl: MetaplusTable, tbl2: MetaplusTable = None):
        self.deleted_variables = [self.target_variable]
        self.target_table = tbl.table_name
        df = tbl.df.drop(self.target_variable)
        return df
