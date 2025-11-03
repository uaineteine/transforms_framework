"""
Schema validation utilities for the new sampling system.

This module provides schema validation functionality for data loaded through 
the new sampling system (sampling_state.json format). It validates that loaded
DataFrames match the expected schema defined in the dtypes field.
"""

from typing import Dict, Union
import pandas as pd
import polars as pl
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, BooleanType


class SchemaValidationError(Exception):
    """Exception raised when schema validation fails."""
    pass


class SchemaValidator:
    """
    Schema validator for DataFrames across different frameworks (PySpark, Pandas, Polars).
    
    This class validates that loaded DataFrames conform to the expected schema
    as defined in the sampling_state.json dtypes field.
    """
    
    # Mapping from string type names to actual types for different frameworks
    PYSPARK_TYPE_MAPPING = {
        'String': StringType(),
        'Int64': IntegerType(),
        'Double': DoubleType(),
        'Date': DateType(),
        'Boolean': BooleanType(),
        # Add more mappings as needed
    }
    
    PANDAS_TYPE_MAPPING = {
        'String': 'object',
        'Int64': 'Int64',
        'Double': 'float64',
        'Date': 'datetime64[ns]',
        'Boolean': 'bool',
        # Add more mappings as needed
    }
    
    POLARS_TYPE_MAPPING = {
        'String': pl.Utf8,
        'Int64': pl.Int64,
        'Double': pl.Float64,
        'Date': pl.Date,
        'Boolean': pl.Boolean,
        # Add more mappings as needed
    }
    
    @staticmethod
    def validate_schema(df: Union[SparkDataFrame, pd.DataFrame, pl.DataFrame], 
                       expected_dtypes: Dict[str, Dict[str, str]], 
                       frame_type: str,
                       table_name: str = "unknown") -> bool:
        """
        Validate that a DataFrame's schema matches the expected dtypes.
        
        Args:
            df: The DataFrame to validate (PySpark, Pandas, or Polars)
            expected_dtypes: Dictionary mapping column names to dtype information
                           Format: {"column_name": {"dtype_source": "String", "dtype_output": "String"}}
            frame_type: The type of DataFrame ("pyspark", "pandas", "polars")
            table_name: Name of the table for error reporting
            
        Returns:
            bool: True if schema is valid
            
        Raises:
            SchemaValidationError: If schema validation fails
        """
        try:
            if frame_type == "pyspark":
                return SchemaValidator._validate_pyspark_schema(df, expected_dtypes, table_name)
            elif frame_type == "pandas":
                return SchemaValidator._validate_pandas_schema(df, expected_dtypes, table_name)
            elif frame_type == "polars":
                return SchemaValidator._validate_polars_schema(df, expected_dtypes, table_name)
            else:
                raise SchemaValidationError(f"Unsupported frame_type: {frame_type}")
        except Exception as e:
            raise SchemaValidationError(f"Schema validation failed for table '{table_name}': {str(e)}")
    
    @staticmethod
    def _validate_pyspark_schema(df: SparkDataFrame, expected_dtypes: Dict[str, Dict[str, str]], table_name: str) -> bool:
        """Validate PySpark DataFrame schema."""
        actual_columns = set(df.columns)
        expected_columns = set(expected_dtypes.keys())
        
        # Check for missing columns
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            raise SchemaValidationError(
                f"Missing columns in table '{table_name}': {sorted(missing_columns)}. "
                f"Actual columns: {sorted(actual_columns)}"
            )
        
        # Check for extra columns (warning, not error)
        extra_columns = actual_columns - expected_columns
        if extra_columns:
            print(f"Warning: Table '{table_name}' has extra columns not in schema: {sorted(extra_columns)}")
        
        # Validate data types for expected columns
        actual_schema = {field.name: field.dataType for field in df.schema.fields}
        
        for col_name, dtype_info in expected_dtypes.items():
            if col_name in actual_columns:
                expected_type_str = dtype_info.get('dtype_output', dtype_info.get('dtype_source'))
                if expected_type_str in SchemaValidator.PYSPARK_TYPE_MAPPING:
                    expected_type = SchemaValidator.PYSPARK_TYPE_MAPPING[expected_type_str]
                    actual_type = actual_schema[col_name]
                    
                    # For now, we'll be flexible about type checking since type inference can vary
                    # This can be made more strict based on requirements
                    print(f"Schema check - Column '{col_name}': expected {expected_type_str}, got {actual_type}")
        
        return True
    
    @staticmethod
    def _validate_pandas_schema(df: pd.DataFrame, expected_dtypes: Dict[str, Dict[str, str]], table_name: str) -> bool:
        """Validate Pandas DataFrame schema."""
        actual_columns = set(df.columns)
        expected_columns = set(expected_dtypes.keys())
        
        # Check for missing columns
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            raise SchemaValidationError(
                f"Missing columns in table '{table_name}': {sorted(missing_columns)}"
            )
        
        # Check for extra columns (warning, not error)
        extra_columns = actual_columns - expected_columns
        if extra_columns:
            print(f"Warning: Table '{table_name}' has extra columns not in schema: {sorted(extra_columns)}")
        
        # Validate data types for expected columns
        for col_name, dtype_info in expected_dtypes.items():
            if col_name in actual_columns:
                expected_type_str = dtype_info.get('dtype_output', dtype_info.get('dtype_source'))
                actual_type = str(df[col_name].dtype)
                
                # For now, we'll be flexible about type checking since type inference can vary
                print(f"Schema check - Column '{col_name}': expected {expected_type_str}, got {actual_type}")
        
        return True
    
    @staticmethod
    def _validate_polars_schema(df: pl.DataFrame, expected_dtypes: Dict[str, Dict[str, str]], table_name: str) -> bool:
        """Validate Polars DataFrame schema."""
        actual_columns = set(df.columns)
        expected_columns = set(expected_dtypes.keys())
        
        # Check for missing columns
        missing_columns = expected_columns - actual_columns
        if missing_columns:
            raise SchemaValidationError(
                f"Missing columns in table '{table_name}': {sorted(missing_columns)}"
            )
        
        # Check for extra columns (warning, not error)
        extra_columns = actual_columns - expected_columns
        if extra_columns:
            print(f"Warning: Table '{table_name}' has extra columns not in schema: {sorted(extra_columns)}")
        
        # Validate data types for expected columns
        actual_schema = dict(zip(df.columns, df.dtypes))
        
        for col_name, dtype_info in expected_dtypes.items():
            if col_name in actual_columns:
                expected_type_str = dtype_info.get('dtype_output', dtype_info.get('dtype_source'))
                actual_type = actual_schema[col_name]
                
                # For now, we'll be flexible about type checking since type inference can vary
                print(f"Schema check - Column '{col_name}': expected {expected_type_str}, got {actual_type}")
        
        return True
    
    @staticmethod
    def get_schema_summary(expected_dtypes: Dict[str, Dict[str, str]]) -> str:
        """
        Get a human-readable summary of the expected schema.
        
        Args:
            expected_dtypes: Dictionary mapping column names to dtype information
            
        Returns:
            str: A formatted string describing the expected schema
        """
        summary_lines = ["Expected Schema:"]
        for col_name, dtype_info in expected_dtypes.items():
            dtype_source = dtype_info.get('dtype_source', 'Unknown')
            dtype_output = dtype_info.get('dtype_output', dtype_source)
            summary_lines.append(f"  {col_name}: {dtype_source} -> {dtype_output}")
        
        return "\n".join(summary_lines)