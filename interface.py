import secrets

from sqlalchemy import inspect
from flask import Flask
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView
import tomli

from lib.d3models import D3EditionMetadata, D3TableMetadata, D3VariableMetadata, D3VariableGroup
from lib.connection import build_connections


with open("pipeline_config.toml", "rb") as f:
    config = tomli.load(f)


_, workspace_session_maker, _ = build_connections(config)

WorkspaceSession = workspace_session_maker()

db = WorkspaceSession()

app = Flask(__name__)

# set optional bootswatch theme
app.config['FLASK_ADMIN_SWATCH'] = 'cerulean'


secret_key = secrets.token_hex(16)
# example output, secret_key = 000d88cd9d90036ebdd237eb6b0db000
app.config['SECRET_KEY'] = secret_key


def make_view(table_metadata_class):
    class VerboseView(ModelView):
        column_display_pk = True # optional, but I like to see the IDs in the list
        column_hide_backrefs = False
        column_list = [c_attr.key for c_attr in inspect(table_metadata_class).mapper.column_attrs]

    return VerboseView


class TableView(ModelView):
    inline_models = (D3VariableMetadata, D3EditionMetadata, D3VariableGroup)
    column_display_pk = True # optional, but I like to see the IDs in the list
    column_hide_backrefs = False
    column_list = [c_attr.key for c_attr in inspect(D3TableMetadata).mapper.column_attrs]


# TableView = make_view(D3TableMetadata)
VariableView = make_view(D3VariableMetadata)
EditionView = make_view(D3EditionMetadata)

admin = Admin(app, name='D3 Data Pipeline', template_mode='bootstrap3')
admin.add_view(TableView(D3TableMetadata, db))
admin.add_view(VariableView(D3VariableMetadata, db))
admin.add_view(EditionView(D3EditionMetadata, db))


app.run()
