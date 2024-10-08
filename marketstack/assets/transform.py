from marketstack.connectors.postgresql import PostgreSqlClient
from jinja2 import Environment


def execute_template_sql(
    postgresql_client: PostgreSqlClient,
    environment: Environment,
    table_name: str,
        ):
    """
    execute template SQL given jinja environment and postgresql client and a chosen table name

    """
    template = environment.get_template(f"{table_name}.sql")

    exec_sql = f"""
        drop table if exists {table_name};
        create table {table_name} as (
            {template.render()}
        )
    """
    postgresql_client.execute_sql(exec_sql)
    