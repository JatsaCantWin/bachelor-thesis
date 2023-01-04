from pymongo import MongoClient
from pymongo.operations import InsertOne, UpdateOne, DeleteOne

class MongoDatabaseTesting(DatabaseTestingInterface):
    def __init__(self, connection_string: str, csv_file_paths: List[str]):
        self.client = MongoClient(connection_string)
        self.csv_data = {}
        self.collection_names = []
        self.collection_column_names = {}
        self.DATABASE_NAME = "databasetesting"

        for file_path in csv_file_paths:
            collection_name = os.path.splitext(os.path.basename(file_path))[0]
            self.collection_names.append(collection_name)
            self.collection_column_names[collection_name], self.csv_data[collection_name] = self.__parse_csv_data(file_path)

        self.__normalize_collection_column_names()

    def __normalize_collection_column_names(self):
        for database_name, column_names in self.collection_column_names.items():
            column_names = [''.join(c for c in name.strip() if c.isalnum() or c == '_') for name in column_names]
            self.collection_column_names[database_name] = column_names

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

    def __convert_timestamp(self, timestamp_string: str):
        return datetime.strptime(timestamp_string, '%Y-%m-%d %H:%M:%S.%f').timestamp()

    def create(self, rows_created: int = 1, transactions: int = 1):
        db = self.client[self.DATABASE_NAME]
        for collection_name, data in self.csv_data.items():
            for transaction in range(transactions):
                requests = []
                for i in range(rows_created):
                    requests.append(InsertOne({
                        self.collection_column_names[collection_name][0]: self.__convert_timestamp(data[i][0]),
                        self.collection_column_names[collection_name][1]: self.__convert_timestamp(data[i][1]),
                        self.collection_column_names[collection_name][2]: data[i][2],
                        self.collection_column_names[collection_name][3]: data[i][3]
                    }))
                collection = db[collection_name]
                collection.bulk_write(requests, ordered=False)

    def read(self, rows_read: int = 1, transactions: int = 1):
        for collection_name in self.collection_names:
            db = self.client[self.DATABASE_NAME]
            collection = db[collection_name]
            for transaction in range(transactions):
                collection.find().limit(rows_read).next()

    def update(self, rows_updated: int = 1, transactions: int = 1):
        db = self.client[self.DATABASE_NAME]
        for collection_name, data in self.csv_data.items():
            for transaction in range(transactions):
                requests = []
                for i in range(rows_updated):
                    requests.append(UpdateOne({self.collection_column_names[collection_name][0]: self.__convert_timestamp(self.csv_data[collection_name][i][0]), self.collection_column_names[collection_name][1]: self.__convert_timestamp(self.csv_data[collection_name][i][1])}, {'$set': {self.collection_column_names[collection_name][2]: 0}}))
                collection = db[collection_name]
                collection.bulk_write(requests, ordered=False)

    def delete(self, rows_deleted: int = 1, transactions: int = 1):
        db = self.client[self.DATABASE_NAME]
        for collection_name, data in self.csv_data.items():
            for transaction in range(transactions):
                requests = []
                for i in range(rows_deleted):
                    requests.append(DeleteOne({self.collection_column_names[collection_name][0]: self.__convert_timestamp(self.csv_data[collection_name][i][0]), self.collection_column_names[collection_name][1]: self.__convert_timestamp(self.csv_data[collection_name][i][1])}))
                collection = db[collection_name]
                collection.bulk_write(requests, ordered=False)

    def reset(self):
        db = self.client[self.DATABASE_NAME]
        for collection_name in self.collection_names:
            db[collection_name].drop()

    def getName(self) -> str:
        return "MongoDB"

    def __del__(self):
        self.client.close()