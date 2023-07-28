import pandas as pd

from .d3models import D3VariableMetadata


def build_empty_table(variables: list[D3VariableMetadata]) -> pd.DataFrame:
    return pd.DataFrame(
        data=[], 
        columns=["geoid"] + [var.variable_name for var in variables]
    ) 

