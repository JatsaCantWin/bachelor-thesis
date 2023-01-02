#include <iostream>
#include "DatabaseConnectors/SQLiteConnector.h"
#include "DatabaseConnectors/CassandraConnector.h"
#include "DatabaseConnectors/MongoDBConnector.h"

int main(int argc, char** argv)
{
    if (argc != 3) {
        std::cerr << "Error: incorrect number of arguments provided. Expected 2 arguments: a path to an SQLite database and a Cassandra database address" << std::endl;
        return 1;
    }

    std::string sqliteDatabasePath(argv[1]);
    std::string mongoDBConnectionString(argv[2]);

    SQLiteConnector sqLiteConnector(sqliteDatabasePath);
    MongoDBConnector mongoDbConnector(mongoDBConnectionString, "Priamus");

    mongoDbConnector.insertTables(sqLiteConnector.getAllTables());
    return 0;
}
