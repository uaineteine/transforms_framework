#to be renamed
import json
from transformslib.tables.collections.collection import TableCollection 
from transformslib.transforms.base import MacroTransform
from transformslib.transforms.atomiclib import *

macro_log_location = "events_log/job_1/treatments.json"

class Macro:
  def __init__(self,
               macro_transform:MacroTransform,
               input_tables:TableCollection,
               output_tables:list[str],
               input_variables:list[str],
               output_variables:list[str]
              ):
    
    self.macros = macro_transform

    self.input_tables = input_tables
    self.output_tables = output_tables
    self.input_variables = input_variables
    self.output_variables = output_variables

    self.macro_log_loc = macro_log_location

  def apply(self):
    return_frames = self.macros.apply(self.input_tables)

    self.log() #dump log
    
    return return_frames
  
  def log(self):
    json_info = self.__dict__

    with open(self.macro_log_loc, 'w') as f:
      json.dump(json_info, f, indent=2)

class TopBottomCode(Macro):
  def __init__(self,
               input_tables:TableCollection,
               input_variables:list[str],
               max_value:Union[int,float],
               min_value:Union[int,float],
              ):
    
    #for each variable:
    transforms = []
    for var in input_variables:
      var_transforms=[
        ReplaceByCondition(
          column=var,
          op=">=",
          value=max_value,
          replacement=max_value
        ),
        ReplaceByCondition(
          column=var,
          op="<=",
          value=min_value,
          replacement=min_value
        )
      ]
      transforms.extend(var_transforms)

    macro = MacroTransform(
      transforms=transforms,
      Name="TopCode",
      Description="Sets maximum value on variable",
      macro_id="TopCode")

    super().__init__(
      macro_transform=macro,
      input_tables=input_tables,
      output_tables=input_tables.table_names(),
      input_variables=input_variables,
      output_variables=input_variables
    )
