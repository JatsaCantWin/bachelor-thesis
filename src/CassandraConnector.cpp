#include <iostream>
#include "../DataContainers/Column.h"
#include "CassandraConnector.h"

CassandraConnector::CassandraConnector(const std::string& host, const std::string& keyspaceName) {
    this->keyspaceName = keyspaceName;

    connect_future = nullptr;
    cluster = cass_cluster_new();
    session = cass_session_new();

    cass_cluster_set_contact_points(cluster, host.c_str());
    connect_future = cass_session_connect(session, cluster);

    CassError rc = cass_future_error_code(connect_future);

    if (rc != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(connect_future, &message, &message_length);
        fprintf(stderr, "Cassandra connection error: '%.*s'\n", (int)message_length, message);
        exit(1);
    }

    std::string dropKeyspaceStatement;
    dropKeyspaceStatement.append("DROP KEYSPACE IF EXISTS ").append(keyspaceName);
    std::string initializeKeyspaceStatement;
    initializeKeyspaceStatement.append("CREATE KEYSPACE ").append(keyspaceName).append(" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    executeStatement(dropKeyspaceStatement);
    executeStatement(initializeKeyspaceStatement);
}

CassandraConnector::~CassandraConnector() {
    cass_future_free(connect_future);
    cass_session_free(session);
    cass_cluster_free(cluster);
}

int CassandraConnector::executeStatement(const std::string &statement) {
    CassFuture* result_future = cass_session_execute(session, cass_statement_new(statement.c_str(), 0));
    CassError result_error = cass_future_error_code(result_future);
    if (result_error != CASS_OK) {
        std::cout << "Error executing statement: (" << statement << ") " << cass_error_desc(result_error) << std::endl;
        return 1;
    }
    const CassResult* result = cass_future_get_result(result_future);
    cass_result_free(result);
    cass_future_free(result_future);
    return 0;
}

int CassandraConnector::insertTable(Table& table) {
    std::string createTableSql;

    createTableSql.append("CREATE TABLE ").append(keyspaceName).append(".").append(table.getName()).append(" (");
    for (int i = 0; i < table.getColumns().size(); i++)
    {
        createTableSql.append(table.getColumns()[i].getName()).append(" ");
        createTableSql.append(table.getColumns()[i].getCells()[0].getDataTypeCassandra());
        if (i == 0)
            createTableSql.append(" PRIMARY KEY");
        if (i != table.getColumns().size()-1)
            createTableSql.append(", ");
    }
    createTableSql.append(")");
    executeStatement(createTableSql);

    std::string insertDataHeader;
    insertDataHeader.append("INSERT INTO ").append(keyspaceName).append(".").append(table.getName()).append(" (");
    for (int i = 0; i < table.getColumns().size(); i++) {
        insertDataHeader.append(table.getColumns()[i].getName());
        if (i != table.getColumns().size()-1)
            insertDataHeader.append(", ");
    }
    insertDataHeader.append(") VALUES (");

    for (int j = 0; j < table.getColumns()[0].getCells().size(); j++) {
        std::string insertData;
        insertData.append(insertDataHeader);
        for (int i = 0; i < table.getColumns().size(); i++) {
            insertData.append(table.getDataCassandra(i, j));
            if (i != table.getColumns().size()-1)
                insertData.append(", ");
        }
        insertData.append(")");
        executeStatement(insertData);
    }
    return 0;
}

int CassandraConnector::insertTables(const std::vector<Table>& tableList) {
    for (auto table : tableList)
        insertTable(table);
    return 0;
}

Table CassandraConnector::getTable(const std::string &tableName) {
    throw std::logic_error("NotImplemented");
}

std::vector<std::string> CassandraConnector::getTableNames() {
    throw std::logic_error("NotImplemented");
}

std::vector<Table> CassandraConnector::getAllTables() {
    throw std::logic_error("NotImplemented");
}
