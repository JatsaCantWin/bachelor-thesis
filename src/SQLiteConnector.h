#ifndef PRIAMUS_SQLITECONNECTOR_H
#define PRIAMUS_SQLITECONNECTOR_H

#include <sqlite3.h>
#include <string>
#include "IDatabaseConnector.h"
#include "../DataContainers/Table.h"
#include "../DataContainers/Column.h"

class SQLiteConnector: IDatabaseConnector {
private:
    sqlite3 *databasePointer{};
    Table getTable(const std::string& tableName) override;
    std::vector<std::string> getTableNames() override;
    int executeSQL(const std::string& sqlStatement, Table& returnTable);
public:
    explicit SQLiteConnector(const std::string& databasePath);
    ~SQLiteConnector();

    std::vector<Table> getAllTables() override;
    int insertTables(const std::vector<Table>& tableList) override;
};


#endif //PRIAMUS_SQLITECONNECTOR_H
