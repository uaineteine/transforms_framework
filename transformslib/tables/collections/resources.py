from transformslib.engine import get_engine, get_spark
from transformslib.templates.pathing import apply_formats
from transformslib.tables.metaframe import MetaFrame

import pandas as pd
import polars as pl
from pyspark.sql import DataFrame
from pyspark.sql.functions import reduce

import os

def load_specific_ent_map(id_group:int) -> MetaFrame:
    """
    Docstring for load_specific_ent_map
    
    :param id_group: The id group to load
    :type id_group: int
    :return: The entity map of that group
    :rtype: MetaFrame
    """

    map_path = apply_formats(os.getenv("TNSFRMS_RES_LOC"))
    map_path = f"{map_path}/dss_entity_map_view/id_group={id_group}/"

    tn = "entity_map_{id_group}"
    engine = get_engine()
    df = MetaFrame.load(map_path, format="parquet", table_name=tn, frame_type=engine, spark=get_spark())
    return df

def load_ent_map(id_groups:list[int]) -> MetaFrame:
    """
    load the entity map from the resources location

    Args: lists[int]

    Returns: Metaframe of the id groups
    """
    #deduplicate the id groups first
    #this will reduce the amount of frames to concat
    id_groups = list(set(id_groups))

    #if using multiple id groups
    if len(id_groups) > 1:
        frames = []
        for id in id_groups:
            frames.append(load_specific_ent_map(id))
        
        #append these frames together
        engine = get_engine()
        if engine == "pandas":
            df = pd.concat(frames, ignore_index=True)

        elif engine == "polars":
            df = pl.concat(frames)

        elif engine == "pyspark":
            df = reduce(DataFrame.unionAll, frames)

        else:
            raise NotImplementedError(f"RS001 Entity mapping appendage not implemented for backend '{engine}'")
    #there is only 1 df
    else:
        df = load_specific_ent_map(id)

    #deuplicate for return
    return df.distinct()

    