#include <regex>
#include "SQLiteConnector.h"
#include "../DataContainers/Cell.h"

SQLiteConnector::SQLiteConnector(const std::string& databasePath) {
    sqlite3_open(databasePath.c_str(), &databasePointer);
}

SQLiteConnector::~SQLiteConnector() {
    sqlite3_close(databasePointer);
}

int SQLiteConnector::executeSQL(const std::string& sqlStatement, Table &returnTable) {
    auto sqlCallback = [](void *returnTable, int columnCount, char** fieldContent, char** columnNames) -> int    // * Constructs return string containing the tables that were operated on
    {
        for (int i = 0; i < columnCount; i++)
        {
            if ((*(Table*)returnTable).getColumns().size() <= i)
                (*(Table*)returnTable).addColumn(Column(columnNames[i]));
            (*(Table*)returnTable).addCell(i, Cell::createFromSQLite(fieldContent[i] ? std::string(fieldContent[i]) : std::string("NULL"), "Text"));
        }
        return 0;
    };

    sqlite3_exec(databasePointer, sqlStatement.c_str(), sqlCallback, &returnTable, nullptr);

    return 0;
}

Table SQLiteConnector::getTable(const std::string& tableName) {
    Table returnTable(tableName);
    std::vector<std::string> dataTypes;

    Table tableInfo("Info Table");
    std::string tableInfoSql;
    tableInfoSql.append("PRAGMA TABLE_INFO(").append(tableName).append(")");
    executeSQL(tableInfoSql, tableInfo);

    for (int i = 0; i<tableInfo.getColumns()[2].getCells().size(); i++)
    {
        dataTypes.push_back(tableInfo.getData(2, i));
    }

    Table tableData("Queried Data");
    std::string tableDataSql;
    tableDataSql.append("SELECT * FROM ").append(tableName);
    executeSQL(tableDataSql, tableData);
    for (int i = 0; i < tableData.getColumns().size(); i++)
    {
        returnTable.addColumn(Column(tableInfo.getColumns()[1].getCells()[i].getData()));
        for(int j = 0; j < tableData.getColumns()[i].getCells().size(); j++)
        {
            returnTable.addCell(i, Cell::createFromSQLite(tableData.getData(i, j), dataTypes[i]));
        }
    }

    return returnTable;
}

std::vector<std::string> SQLiteConnector::getTableNames() {
    std::vector<std::string> tableNames;

    Table tableList("Table List");
    std::string tableListSql;
    tableListSql.append("SELECT * FROM sqlite_master;");
    executeSQL(tableListSql, tableList);

    for (int i = 0; i < tableList.getColumns()[1].getCells().size(); i++)
        if (tableList.getData(0, i) == "table")
            tableNames.push_back(tableList.getData(1, i));

    return tableNames;
}

std::vector<Table> SQLiteConnector::getAllTables() {
    std::vector<Table> tableList;

    std::vector<std::string> tableNameList = getTableNames();

    for (const auto& tableName: tableNameList)
    {
        tableList.push_back(getTable(tableName));
    }

    return tableList;
}

int SQLiteConnector::insertTables(const std::vector<Table> &tableList) {
    throw std::logic_error("NotImplemented");
}
