
from src.sql_table import  DummySqlDB
import pandas as pd
import pytest


@pytest.fixture()
def mocking_session():
    class SessionMocker():
        def __init__(self):
            pass

        def execute(self, *args):
            if args:
                return [arg for arg in args]

        def commit(self):
            print('committed')

        def close(self):
            print('closed')

    yield SessionMocker


class TestDummyDB():

    @pytest.fixture()
    # This fixture will only be available within the scope of TestGroup
    def mock(self, mocker):
        mocker.patch('src.sql_table.DummySqlDB._create_engine').return_value = 'test_string'
        mocker.patch('src.sql_table.DummySqlDB.query_sql').return_value = pd.DataFrame({'user': ['test']})

    def test_transform(self, mock):

        cls = DummySqlDB()
        actual_features = cls.transform()
        expect_frame = pd.DataFrame({'user': ['test'],'test':['test']})
        pd.testing.assert_frame_equal(actual_features, expect_frame)


    def test_insert_single_row(self, mock, mocking_session, capsys):
        cls = DummySqlDB()
        cls.session_maker = mocking_session
        cls.run_statement('table_name', {'row_data': 'test'})
        out, err = capsys.readouterr()
        assert out == "committed\nclosed\n"