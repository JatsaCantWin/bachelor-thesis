#ifndef PRIAMUS_MONGODBCONNECTOR_H
#define PRIAMUS_MONGODBCONNECTOR_H

#include <mongoc.h>
#include <string>
#include "IDatabaseConnector.h"
#include "../DataContainers/Table.h"
#include "../DataContainers/Column.h"

class MongoDBConnector: public IDatabaseConnector {
private:
    std::string databaseName;

    mongoc_client_t* client{};
    mongoc_database_t* database{};

    Table getTable(const std::string& tableName) override;
    std::vector<std::string> getTableNames() override;
    int insertTable(Table& table);
    int insertDocument(mongoc_collection_t* collection, const bson_t* document);
public:
    explicit MongoDBConnector(const std::string& host, const std::string& databaseName);
    ~MongoDBConnector();

    std::vector<Table> getAllTables() override;
    int insertTables(const std::vector<Table>& tableList) override;
};


#endif //PRIAMUS_MONGODBCONNECTOR_H
