#import mapping types
from transformslib.dag.maps import *

from transformslib.tables.collections.collection import TableCollection
from typing import List
from abc import ABC, abstractmethod
from transformslib.tables.metaframe import MetaFrame
import pyspark.sql.functions as f
from transformslib.events import PipelineEvent

class Transform(ABC):
    # override_column_maps adds extra mapping to column lineage to track column over time
    # treatment_mappings identifies certain mappings as mappings where treatments have been applied
    # - these need to be followed till the end of their lineage where their checks will be applied
    def __init__(self, name: str, input_tables: List[str], output_tables: List[str], column_mapping_type: int=TransformColumnMappingType.ALL_INPUT_COLUMNS,
        treatment_mappings: List[TransformTableColumnMapping]=None, override_column_maps: List[TransformTableColumnMapping]=None):
        self.input_tables = input_tables
        self.output_tables = output_tables
        self.name = name
        self.column_mapping_type = column_mapping_type
        self.treatment_mappings = [] if treatment_mappings is None else treatment_mappings
        self.extra_column_maps = [] if override_column_maps is None else override_column_maps

        if column_mapping_type >= len(input_tables):
            raise ValueError("Invalid column_mapping_type value too large to correspond to an input table")

    @abstractmethod
    def apply_transform(self, dataset_transforms: 'DatasetTransforms'):
        pass


# note: in theory this harnesses lazy eval. i dont have spark so i cant actually test lolll
# maybe i should have inherited from table_collection bc of the getitem after all idk
class DatasetTransforms():
    def __init__(self, table_collection: TableCollection):
        self.input_tables = table_collection
        self.table_dfs = {
            # list is version history of each table_name. index is the version number of the table
            table_name: [table_collection[table_name]] for table_name in table_collection.get_table_names()
        }
        # table to table edges (incl table version) - should always form complete dag
        self.table_mappings = []
        # column to column edges for transformed columns ONLY - may be incomplete if table maps are used in the middle
        self.column_mappings = []
        # identify which column to column edges are considered treatments and will be propogated to the end to apply tests on
        self.treatment_mappings = []
        self.transforms = []
        self.dataset_events = []

    # TODO: a bit too much code for comfort - is there a cleaner way to do this, or is the idea a bit too convoluted
    def _process_transform_graph(self, transform: Transform):
        overwritten_columns = {(column_mapping.from_table, column_mapping.from_column_name) for column_mapping in transform.extra_column_maps}
        column_mappings_to_add = []

        def get_inp_table_version(inp_tablename: str) -> int:
            max_inp_table_version = len(self.table_dfs[inp_tablename])
            return max_inp_table_version - 1 if inp_tablename in transform.output_tables else max_inp_table_version
        
        def get_out_table_version(out_tablename: str) -> int:
            return len(self.table_dfs[out_tablename])

        for inp_i, inp_table in enumerate(transform.input_tables):
            for out_table in transform.output_tables:
                # processs table connections
                self.table_mappings.append(TableMapping(inp_table, get_inp_table_version(inp_table), out_table, get_out_table_version(out_table)))
        
                # process column connections
                # add columns only if column_mapping_type is ALL_INPUT_COLUMNS or current inp_table is specified input table to map columns from
                if transform.column_mapping_type == TransformColumnMappingType.ALL_INPUT_COLUMNS or transform.column_mapping_type == inp_i:
                    # checking columns should not trigger pyspark compute
                    inp_table_columns = self.table_dfs[inp_table].columns
                    column_mappings_to_add += [
                        TableColumnMapping(inp_table, get_inp_table_version(inp_table), col, out_table, get_out_table_version(out_table), col) for col in inp_table_columns if col not in overwritten_columns
                    ]
        
        # add overwritten columns to column mappings after converting to TableColumnMapping with version
        column_mappings_to_add += [TableColumnMapping(
            column_mapping.from_table,
            get_inp_table_version(column_mapping.from_table),
            column_mapping.from_column_name,
            column_mapping.to_table,
            get_out_table_version(column_mapping.to_table),
            column_mapping.to_column_name
        ) for column_mapping in transform.extra_column_maps]
        
        # maybe not the most efficient but shouldnt be too big a deal here TODO: improve efficiency of check
        def check_if_valid_mapping(column_map: TableColumnMapping) -> bool:
            return (
                column_map.from_column_name in self.table_dfs[column_map.from_table][column_map.from_table_ver] and 
                column_map.to_column_name in self.table_dfs[column_map.to_table][column_map.to_table_ver]
            )
        
        if not all(check_if_valid_mapping(col_map) for col_map in column_mappings_to_add):
            raise ValueError(f"Not all mappings are valid: {[col_map for col_map in column_mappings_to_add if not check_if_valid_mapping(col_map)]}")
        

        if not all(treatment_mapping in set(column_mappings_to_add) for treatment_mapping in transform.treatment_mappings):
            raise ValueError(f"Not all treatment mappings exist as column mappings: {
                [treatment_mapping for treatment_mapping in transform.treatment_mappings if treatment_mapping not in set(column_mappings_to_add)]
            }")

        self.column_mappings += column_mappings_to_add
        self.treatment_mappings += transform.treatment_mappings
        

    # this relies on lazy eval to not actually do compute lol. should work in theory i think
    # alternatively we can store the function instructions as string and eval later - this might be a bit sus
    # are there any other options to ensure we don't compute everything on the spot
    def _process_transform(self, transform: Transform):
        for out_name, out_df in transform.apply_transform(self).items():
            self.table_dfs.setdefault(out_name, []).append(out_df)

    def add_transform(self, transform: Transform):
        self.transforms.append(transform)
        self._process_transform(transform)
        self._process_transform_graph(transform)
        self.dataset_events.append(PipelineEvent("transform", f"applying transform {transform.name}"))
    
    def get_table(self, table_name: str, version: int = None) -> MetaFrame:
        if table_name not in self.table_dfs:
            raise KeyError(f"Table '{table_name}' not found")
        
        if version is None:
            version = len(self.table_dfs[table_name]) - 1

        return self.table_dfs[table_name][version]
    
    # TODO: how do we specify the actual output tables we want lol. this will give back input tables as well :P
    def get_output_tables(self) -> TableCollection:
        return TableCollection([table_dfs[-1] for table_dfs in self.table_dfs.values()])

    def __getitem__(self, name: str):
        return self.get_table(name)

    def get_dataset_dag(self):
        return self.table_mappings, self.column_mappings, self.treatment_mappings
    
    def get_events(self):
        self.dataset_events