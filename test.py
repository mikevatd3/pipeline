import pandas as pd
import tomli

from lib.d3models import get_variable_metadata
from lib.connection import build_connections

with open("pipeline_config.toml", "rb") as f:
    config = tomli.load(f)

_, workspace_session_maker, _ = build_connections(config)

WorkspaceSession = workspace_session_maker()

with WorkspaceSession() as db:
    df = pd.DataFrame(
        data=[], 
        columns=["geoid"] + [var.variable_name for var in get_variable_metadata(db, "b01980")]
    ) 

print(df) 

