from transformslib.templates.pathing import apply_formats

def load_specific_ent_map(id_group:int) -> MetaFrame:
    """
    Docstring for load_specific_ent_map
    
    :param id_group: The id group to load
    :type id_group: int
    :return: The entity map of that group
    :rtype: MetaFrame
    """

    map_path = apply_formats(os.getenv("TNSFRMS_RES_LOC"))
    map_path = f"{map_path}/dss_entity_map_view/id_group={id_group}"

    tn = "entity_map_{id_group}"
    df = MetaFrame.load(map_path, format="parquet", table_name=tn, spark=get_spark())
    return df

def load_ent_map(id_groups:list[int]) -> MetaFrame:
    """
    load the entity map from the resources location

    Args: lists[int]

    Returns: Metaframe of the id groups
    """
    frames = []
    for id in id_groups:
        frames.append(load_specific_ent_map(id))
    
    #append these frames together