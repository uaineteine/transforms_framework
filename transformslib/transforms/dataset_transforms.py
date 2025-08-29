from dataset_transforms_base import * # TODO: import just the stuff i need :P
from abc import ABC, abstractmethod

# idea i think we use duck typing for testable transforms
class SimpleColumnTransform(Transform):
    def __init__(self, transform_name, df_name, column_name, is_treatment: bool=False):
        super().__init__(
            transform_name, [df_name], [df_name],
            treatment_mappings=[TransformTableColumnMapping(
                df_name, column_name, df_name, column_name
            )] if is_treatment else None
        )
        self.df_name = df_name
        self.column_name = column_name

    def apply_transform(self, dataset_transforms):
        # won't lie im not really sure this bit works lol --- i think it should in theory but no pyspark outside of ngd,,,
        return {
            self.df_name: dataset_transforms[self.df_name].withColumn(self.column_name, self.col_expr())
        }
    
    @abstractmethod
    def col_expr(self):
        pass

class TopBottomCodeTransform(SimpleColumnTransform):
    def __init__(self, df_name, column_name, min, max):
        super().__init__(f"Top bottom code {min} {max}", df_name, column_name)
        self.column_name = column_name
        self.min = min
        self.max = max
    
    # um i wont lie im not sure if this will work... hopefully?
    def col_expr(self):
        return (
            f.when(f.col(self.column_name) > self.max, f.lit(self.max))
            .when(f.col(self.column_name) < self.min, f.lit(self.min))
            .otherwise(f.col(self.column_name))
        )
    
class SHA256HashTransform(SimpleColumnTransform):
    def __init__(self, df_name, column_name):
        super().__init__(f"SHA256 hash", df_name, column_name)
        self.column_name = column_name
    
    # um i wont lie im not sure if this will work... hopefully?
    def col_expr(self):
        return f.sha2(f.col(self.column_name).cast("string"), 256)

class JoinTransform(Transform):
    def __init__(self, df1_name, df2_name, df_out_name, **join_kwargs):
        super().__init__(f"Join {str(join_kwargs)}", [df1_name], [df2_name])
        self.df1_name = df1_name
        self.df2_name = df2_name
        self.df_out_name = df_out_name
        self.join_kwargs = join_kwargs

    # this is me trying to example this for now. we should be able to wrap this in something far more simple for one to one dataframe transformations
    def apply_transform(self, dataset_transforms: 'DatasetTransforms'):
            return {
                self.df_out_name: dataset_transforms[self.df1_name].join(dataset_transforms[self.df2_name], **self.join_kwargs)
            }

if __name__ == "__main__":
    # haha pretend i actually imporated some dataframes
    table_collection = TableCollection()
    dataset_transforms = DatasetTransforms(table_collection)
    
    dataset_transforms.add_transform(TopBottomCodeTransform("dataframe1", "income", 0, 100000))
    dataset_transforms.add_transform(SHA256HashTransform("dataframe2", "person_id"))
    dataset_transforms.add_transform(JoinTransform("dataframe1", "dataframe2", "merged_dataframe", how="outer"))

    output_tables = dataset_transforms.get_output_tables()