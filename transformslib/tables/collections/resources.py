from transformslib.engine import get_engine, get_spark
from transformslib.templates.pathing import apply_formats
from transformslib.tables.metaframe import MetaFrame
from multitable import MultiTable

import pandas as pd
import polars as pl

from functools import reduce

import os

def concat(frames:list[MultiTable], engine:str) -> MultiTable:
    if not frames:
        raise ValueError("No frames to concatenate")
    native_frames = [f.df for f in frames]

    if engine == "pandas":
        combined = pd.concat(native_frames, ignore_index=True)

    elif engine == "polars":
        combined = pl.concat(native_frames)

    elif engine == "pyspark":
        # Safe union across all frames
        if len(native_frames) == 1:
            combined = native_frames[0]
        else:
            combined = reduce(lambda df1, df2: df1.union(df2), native_frames)

    else:
        raise NotImplementedError(
            f"RS400 Metaframe appendage not implemented for backend '{engine}'"
        )
    
    return MultiTable(combined, src_path=frames[0].src_path, table_name=frames[0].table_name, frame_type=engine)

def load_specific_ent_map(id_group:int) -> MultiTable:
    """
    Docstring for load_specific_ent_map
    
    :param id_group: The id group to load
    :type id_group: int
    :return: The entity map of that group
    :rtype: MultiTable
    """
    #set the map paths
    map_path = apply_formats(os.getenv("TNSFRMS_RES_LOC"))
    map_path = map_path.replace("{id_group}", str(id_group))
    
    #gather resource format
    fmt = os.getenv("TNSFRMS_RES_TYPE", "parquet")

    tn = f"entity_map_{id_group}"
    engine = get_engine()
    df = MultiTable.load(map_path, format=fmt, table_name=tn, frame_type=engine, auto_lowercase=True, spark=get_spark())
    return df

def load_ent_map(id_groups:list[int]) -> MetaFrame:
    """
    load the entity map from the resources location

    Args: lists[int]

    Returns: Metaframe of the id groups
    """
    if len(id_groups) == 0:
        raise ValueError("RS100 ID groups cannot be empty as a list")
    
    #deduplicate the id groups first
    #this will reduce the amount of frames to concat
    id_groups = list(set(id_groups))

    
    engine = get_engine()
    
    #if using multiple id groups
    if len(id_groups) > 1:
        frames = []
        for id in id_groups:
            frames.append(load_specific_ent_map(id))
        
        #append these frames together
        df = concat(frames, engine)
    
    #there is only 1 df
    else:
        df = load_specific_ent_map(id_groups[0])

    df.distinct()
    
    return MetaFrame(df)
