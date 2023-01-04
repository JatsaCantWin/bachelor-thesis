import psycopg2

class PostgresDatabaseTesting(DatabaseTestingInterface):
    def __init__(self, connection_string: str, csv_file_paths: List[str]):
        self.connection = psycopg2.connect(connection_string)
        self.csv_data = {}
        self.table_names = []
        self.table_column_names = {}

        for file_path in csv_file_paths:
            table_name = os.path.splitext(os.path.basename(file_path))[0]
            self.table_names.append(table_name)
            self.table_column_names[table_name], self.csv_data[table_name] = self.__parse_csv_data(file_path)

        self.__normalize_table_column_names()
        self.__create_tables()

    def __create_tables(self):
        cursor = self.connection.cursor()
        for table_name in self.table_names:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            column_definitions = []
            column_definitions.append(f"{self.table_column_names[table_name][0]} timestamp NOT NULL")
            column_definitions.append(f"{self.table_column_names[table_name][1]} timestamp NOT NULL")
            column_definitions.append(f"{self.table_column_names[table_name][2]} numeric NOT NULL")
            column_definitions.append(f"{self.table_column_names[table_name][3]} integer")
            column_definitions_str = ", ".join(column_definitions)
            cursor.execute(f"CREATE TABLE {table_name} ({column_definitions_str})")
        self.connection.commit()

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
                cursor = self.connection.cursor()
                for i in range(rows_created):
                    cursor.execute(f"INSERT INTO {table_name} VALUES (%s, %s, %s, %s)", data[i+transaction*rows_created])
                self.connection.commit()

    def read(self, rows_read: int = 1, transactions: int = 1):
        for table_name in self.table_names:
            for transaction in range(transactions):
                cursor = self.connection.cursor()
                cursor.execute(f"SELECT * FROM {table_name} LIMIT {rows_read}")
                cursor.fetchone()

    def update(self, rows_updated: int = 1, transactions: int = 1):
        for table_name in self.table_names:
            for transaction in range(transactions):
                cursor = self.connection.cursor()
                for i in range(rows_updated):
                    cursor.execute(f"UPDATE {table_name} SET {self.table_column_names[table_name][2]} = 0 WHERE {self.table_column_names[table_name][0]} = %s", (self.csv_data[table_name][i+transaction*rows_updated][0],))
                self.connection.commit()

    def delete(self, rows_deleted: int = 1, transactions: int = 1):
        for table_name in self.table_names:
            for transaction in range(transactions):
                cursor = self.connection.cursor()
                for i in range(rows_deleted):
                    cursor.execute(f"DELETE FROM {table_name} WHERE {self.table_column_names[table_name][0]} = %s", (self.csv_data[table_name][i+transaction*rows_deleted][0],))
                self.connection.commit()

    def reset(self):
        cursor = self.connection.cursor()
        for table_name in self.table_names:
            cursor.execute(f"DROP TABLE {table_name}")
        self.connection.commit()
        self.__create_tables()

    def getName(self) -> str:
        return "PostgreSQL"

    def __del__(self):
        self.connection.close()