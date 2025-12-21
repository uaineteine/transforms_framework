from transformslib.engine import get_engine, get_spark
from transformslib.templates.pathing import apply_formats
from transformslib.tables.metaframe import MetaFrame
from multitable import MultiTable, concatlist

import os

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
        df = concatlist(frames, engine)
    
    #there is only 1 df
    else:
        df = load_specific_ent_map(id_groups[0])

    #deduplicate the entity map
    df.distinct()

    #rename the entity map
    df.table_name = "entity_map"
    
    return MetaFrame(df)
