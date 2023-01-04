import cassandra
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, BatchStatement
from typing import List, Tuple

class CassandraDatabaseTesting(DatabaseTestingInterface):
    def __init__(self, connection_string: str, csv_file_paths: List[str]):
        self.cluster = Cluster([connection_string])
        self.session = self.cluster.connect()
        self.csv_data = {}
        self.table_names = []
        self.table_column_names = {}
        self.KEYSPACE_NAME = "databaseTesting"

        for file_path in csv_file_paths:
            table_name = os.path.splitext(os.path.basename(file_path))[0]
            self.table_names.append(table_name)
            self.table_column_names[table_name], self.csv_data[table_name] = self.__parse_csv_data(file_path)

        self.__normalize_table_column_names()
        self.__execute_simple_statement(f"DROP KEYSPACE IF EXISTS {self.KEYSPACE_NAME}")
        self.__create_tables()

    def __execute_simple_statement(self, query: str, parameters: Tuple = ()):
        statement = SimpleStatement(query, consistency_level=cassandra.ConsistencyLevel.ONE)
        self.session.execute(statement, parameters)

    def __execute_batch_statement(self, statements: List[str], parameters_list: List[Tuple[Tuple]]):
        batch = BatchStatement(consistency_level=cassandra.ConsistencyLevel.ONE)
        for statement, parameters in zip(statements, parameters_list):
            batch.add(statement, parameters)
        self.session.execute(batch)

    def __create_tables(self):
        self.__execute_simple_statement(f"CREATE KEYSPACE {self.KEYSPACE_NAME} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
        self.__execute_simple_statement(f"USE {self.KEYSPACE_NAME}")
        for table_name in self.table_names:
            self.__execute_simple_statement(f"CREATE TABLE IF NOT EXISTS {table_name} ({self.table_column_names[table_name][0]} timestamp, {self.table_column_names[table_name][1]} timestamp, {self.table_column_names[table_name][2]} float, {self.table_column_names[table_name][3]} int, PRIMARY KEY ({self.table_column_names[table_name][0]}, {self.table_column_names[table_name][1]}))")

    def __normalize_table_column_names(self):
        for table_name, column_names in self.table_column_names.items():
            column_names = [''.join(c for c in name.strip() if c.isalnum() or c == '_') for name in column_names]
            self.table_column_names[table_name] = column_names

    def __parse_csv_data(self, file_path: str) -> Tuple[List[str], List[Tuple[str, str, float, int]]]:
        MAX_DATA_READ = 1000
        with open(file_path, "r") as f:
            reader = csv.reader(f, delimiter="|")
            column_names = next(reader)
            rows = itertools.islice(reader, MAX_DATA_READ)
            data = []
            for row in rows:
                row[0] = row[0].replace(",", ".")
                row[1] = row[1].replace(",", ".")
                data.append(tuple(row))

        return column_names, data

    def create(self, rows_created: int = 1, transactions: int = 1):
        for table_name, data in self.csv_data.items():
            for transaction in range(transactions):
                statements = []
                parameters_list = []
                for i in range(rows_created):
                    statements.append(f"INSERT INTO {table_name} ({self.table_column_names[table_name][0]}, {self.table_column_names[table_name][1]}, {self.table_column_names[table_name][2]}, {self.table_column_names[table_name][3]}) VALUES (%s, %s, %s, %s)")
                    parameters_list.append((data[i][0], data[i][1], float(data[i][2]), int(data[i][3])))

                self.__execute_batch_statement(statements=statements, parameters_list=parameters_list)

    def read(self, rows_read: int = 1, transactions: int = 1):
        for table_name in self.table_names:
            for transaction in range(transactions):
                statement = f"SELECT * FROM {table_name} LIMT {rows_read}"
                self.session.execute(statement)

    def update(self, rows_updated: int = 1, transactions: int = 1):
        for table_name, data in self.csv_data.items():
            for transaction in range(transactions):
                statements = []
                parameters_list = []
                for i in range(rows_updated):
                    statements.append(f"UPDATE {table_name} SET {self.table_column_names[table_name][2]} = %s WHERE {self.table_column_names[table_name][0]} = %s AND {self.table_column_names[table_name][1]} = %s")
                    parameters_list.append((float(self.csv_data[table_name][i][2]), self.csv_data[table_name][i][0], self.csv_data[table_name][i][1]))

                self.__execute_batch_statement(statements=statements, parameters_list=parameters_list)

    def delete(self, rows_deleted: int = 1, transactions: int = 1):
        for table_name, data in self.csv_data.items():
            for transaction in range(transactions):
                statements = []
                parameters_list = []
                for i in range(rows_deleted):
                    statements.append(f"DELETE FROM {table_name} WHERE {self.table_column_names[table_name][0]} = %s AND {self.table_column_names[table_name][1]} = %s")
                    parameters_list.append((self.csv_data[table_name][i][0], self.csv_data[table_name][i][1]))

                self.__execute_batch_statement(statements=statements, parameters_list=parameters_list)

    def reset(self):
        self.__execute_simple_statement(f"DROP KEYSPACE {self.KEYSPACE_NAME}")
        self.__create_tables()

    def getName(self) -> str:
        return "Cassandra"

    def __del__(self):
        self.session.close()