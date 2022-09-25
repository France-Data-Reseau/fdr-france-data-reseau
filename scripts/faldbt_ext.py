'''
Fal extension :
- allows to write (from pandas Dataframe) any table the way fal does, not only DBT statically defined-models and sources

install dbt & fal
python
import faldbt_ext
test()
'''

from uuid import uuid4
import sqlalchemy
from dbt.adapters.sql import SQLAdapter
import six
from faldbt.lib import (_execute_sql)

def _connection_name(prefix: str, obj, _hash: bool = True):
    # HACK: we need to include uniqueness (UUID4) to avoid clashes
    return f"{prefix}:{hash(str(obj)) if _hash else obj}:{uuid4()}"

def _create_engine_from_connection(adapter: SQLAdapter):
    if adapter.type() == "postgres":
        url_string = "postgresql+psycopg2://"
    else:
        # TODO: add special cases as needed
        ##logger.warn("No explicit url string for adapter {}", adapter.type())
        url_string = f"{adapter.type()}://"

    connection = adapter.connections.get_thread_connection()
    return sqlalchemy.create_engine(url_string, creator=lambda: connection.handle)

'''
if_exists : replace (first DROP CASCADE to avoid exploding on dependent objects), append, fail
'''
def write_table(data, table_name, schema, model_for_connection, adapter, if_exists="replace"):
    # _write_relation(adapter, data, relation, dtype=dtype)
    dtype = None
    #with _existing_or_new_connection(
    #        adapter, _connection_name("write_target", relation, _hash=False), True
    #):
    drop_cascade_stmt = f"drop table if exists \"{schema}\".\"{table_name}\" cascade"
    # 202209 fal code :
    #_execute_sql(
    #    adapter,
    #    six.text_type(drop_cascade_stmt).strip(),
    #    new_conn=False
    #)
    with adapter.connection_named(_connection_name("write_table", model_for_connection, _hash=False)):
        adapter.execute(drop_cascade_stmt, auto_begin=True, fetch=True)
        print('drop_cascade_stmt', drop_cascade_stmt)

        engine = _create_engine_from_connection(adapter)

        rows_affected = data.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            dtype=dtype,
        )

def test():
    from faldbt.lib import (_get_adapter) # _write_relation # _existing_or_new_connection, _connection_name, _create_engine_from_connection
    from fal import FalDbt
    profiles_dir = "~/.dbt"
    project_dir=".."
    faldbt = FalDbt(profiles_dir=profiles_dir, project_dir=project_dir)
    config = faldbt._config
    profile_target = faldbt._config.target_name

    test_relation = faldbt.ref('apcom_def_supportaerien_example')
    model_for_connection = faldbt._model('apcom_def_supportaerien_example', None)
    schema = model_for_connection.schema
    #model_for_connection_and_schema = faldbt._source('fdr_import', 'fdr_import_resource', None)
    adapter = _get_adapter(project_dir, profiles_dir, profile_target, config=config)
    write_table(test_relation, 'testwritedyn', schema, model_for_connection, adapter)