from typing import Any, Union, Dict

import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker

class DummySqlDB():
    """MS SQL Server connection class"""

    def __init__(self):
        self.engine = self._create_engine()
        self.session_maker = sessionmaker(bind=self.engine)

    def _create_engine(self) -> Any:
        """Create SQLAlchemy engine"""
        database_url = 'dummy connect string'
        return sqlalchemy.create_engine(
            database_url, fast_executemany=True, pool_pre_ping=True
        )

    def query_sql(
        self, sql: Union[str, Dict], **kwargs
    ) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:

        if isinstance(sql, str):
            return pd.read_sql(sql, con=self.engine, **kwargs)
        if isinstance(sql, dict):
            data = dict()
            for name, query in sql.items():
                data[name] = pd.read_sql(**query, con=self.engine)
            return data
        raise ValueError("sql argument must be str or dict")

    def transform(self):
      df = self.query_sql("select * from MYTABLE")
      df['test'] = 'test'
      return df

    def run_statement(self, statement, dicts):

        string = f"INSERT INTO {statement} ({', '.join(dicts.keys())}) VALUES (:{', :'.join(dicts.keys())})"

        session = self.session_maker()
        try:
            session.execute(string, dicts)
            session.commit()
        finally:
            session.close()