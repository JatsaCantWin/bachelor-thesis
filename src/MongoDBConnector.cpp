#include <iostream>
#include "../DataContainers/Column.h"
#include "MongoDBConnector.h"

MongoDBConnector::MongoDBConnector(const std::string& host, const std::string& databaseName) {
    this->databaseName = databaseName;

    mongoc_init();
    client = mongoc_client_new(host.c_str());
    database = mongoc_client_get_database(client, databaseName.c_str());
}

MongoDBConnector::~MongoDBConnector() {
    mongoc_database_destroy(database);
    mongoc_client_destroy(client);
    mongoc_cleanup();
}

int MongoDBConnector::insertDocument(_mongoc_collection_t* collection, const bson_t* document) {
    bson_error_t error;
    if (!mongoc_collection_insert(collection, MONGOC_INSERT_NONE, document, nullptr, &error)) {
        fprintf(stderr, "Error inserting document: %s\n", error.message);
        return 1;
    }
    return 0;
}

int MongoDBConnector::insertTable(Table& table) {
    mongoc_collection_t* collection = mongoc_database_get_collection(database, table.getName().c_str());

    for (int j = 0; j < table.getColumns()[0].getCells().size(); j++) {
        bson_t document;
        bson_init(&document);
        for (int i = 0; i < table.getColumns().size(); i++) {
            BSON_APPEND_UTF8(&document, table.getColumns()[i].getName().c_str(), table.getData(i, j).c_str());
        }
        insertDocument(collection, &document);
        bson_destroy(&document);
    }

    mongoc_collection_destroy(collection);
    return 0;
}

int MongoDBConnector::insertTables(const std::vector<Table>& tableList) {
    for (auto table : tableList) {
        insertTable(table);
    }
    return 0;
}

Table MongoDBConnector::getTable(const std::string& tableName) {
    throw std::logic_error("NotImplemented");
}

std::vector<std::string> MongoDBConnector::getTableNames() {
    throw std::logic_error("NotImplemented");
}

std::vector<Table> MongoDBConnector::getAllTables() {
    throw std::logic_error("NotImplemented");
}