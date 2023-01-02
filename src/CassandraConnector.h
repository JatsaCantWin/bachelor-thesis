#ifndef PRIAMUS_CASSANDRACONNECTOR_H
#define PRIAMUS_CASSANDRACONNECTOR_H

#include <cassandra.h>
#include <string>
#include "IDatabaseConnector.h"
#include "../DataContainers/Table.h"
#include "../DataContainers/Column.h"

class CassandraConnector: public IDatabaseConnector {
private:
    std::string keyspaceName;

    CassFuture* connect_future{};
    CassCluster* cluster{};
    CassSession* session{};

    Table getTable(const std::string& tableName) override;
    std::vector<std::string> getTableNames() override;
    int insertTable(Table& table);
    int executeStatement(const std::string& sqlStatement);
public:
    explicit CassandraConnector(const std::string& host, const std::string& keyspaceName);
    ~CassandraConnector();

    std::vector<Table> getAllTables() override;
    int insertTables(const std::vector<Table>& tableList) override;
};


#endif //PRIAMUS_CASSANDRACONNECTOR_H
