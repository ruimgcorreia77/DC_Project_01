from sqlalchemy.engine import URL, CursorResult
from sqlalchemy.dialects import postgresql
from sqlalchemy import (
    create_engine, 
    Table, 
    Column,
    Integer,
    String,
    MetaData,
    Float, 
    select, 
    func, 
)   # https://www.tutorialspoint.com/sqlalchemy/sqlalchemy_core_creating_table.htm
from sqlalchemy.engine import URL
from sqlalchemy.schema import CreateTable 
from sqlalchemy.orm import Session
#https://docs.sqlalchemy.org/en/20/core/exceptions.html#sqlalchemy.exc.NoSuchTableError
from sqlalchemy.exc import NoSuchTableError

class PostgreSqlClient:
    """
    A client for querying postgresql database.
    """

    def __init__(
        self,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int,
    ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )

        self.engine = create_engine(connection_url)

    def get_latest_timestamp(self, table: str, column: str) -> str:
        #to allow for an incremental extract from source, extract the latest timestap from the target table
        try:
            # attempt to reflect existing table
            metadata = MetaData(self.engine)
            target_table = Table(table, metadata, autoload=self.engine)

            with Session(self.engine) as session:                
                latest_date = session.execute(select(func.max(target_table.c[column]))).scalar()
                return latest_date

        except NoSuchTableError:
            return "2024-01-01"
    
    def execute_sql(self, sql: str) -> None:
        self.engine.execute(sql)
    
    def select_all(self, table: Table) -> list[dict]:
        return [dict(row) for row in self.engine.execute(table.select()).all()]

    def create_table(self, metadata: MetaData) -> None:
        """
        Creates table provided in the metadata object
        """
        metadata.create_all(self.engine)

    def drop_table(self, table_name: str) -> None:
        self.engine.execute(f"drop table if exists {table_name};")

    def insert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        metadata.create_all(self.engine)
        insert_statement = postgresql.insert(table).values(data)
        self.engine.execute(insert_statement)

    def overwrite(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        self.drop_table(table.name)
        self.insert(data=data, table=table, metadata=metadata)

    def upsert(self, data: list[dict], table: Table, metadata: MetaData) -> None:
        metadata.create_all(self.engine)
        key_columns = [
            pk_column.name for pk_column in table.primary_key.columns.values()
        ]
        insert_statement = postgresql.insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements=key_columns,
            set_={
                c.key: c for c in insert_statement.excluded if c.key not in key_columns
            },
        )
        self.engine.execute(upsert_statement)

