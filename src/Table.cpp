#include <iostream>
#include <iomanip>
#include "Table.h"

Table::Table(const std::string &tableName) {
    this->tableName = tableName;
}

const std::vector<Column> &Table::getColumns() const {
    return columnContainer;
}

std::string Table::getName() const {
    return tableName;
}

std::string Table::getData(int column, int row) const {
    return getColumns()[column].getCells()[row].getData();
}

std::string Table::getDataCassandra(int column, int row) const {
    return getColumns()[column].getCells()[row].getDataCassandra();
}

void Table::addColumn(const Column& newColumn) {
    columnContainer.push_back(newColumn);
}

void Table::addCell(int column, const Cell &newCell) {
    columnContainer[column].addCell(newCell);
}

void Table::print() {
    for (const auto & column : columnContainer)
    {
        std::cout << std::setw(30) << column.getName() << " | " ;
    }
    std::cout << std::endl;
    for (int j = 0; j < columnContainer[0].getCells().size(); j++) {
        for (int i = 0; i < columnContainer.size(); i++) {
            std::cout << std::setw(30) << getData(i, j) << " | ";
        }
        std::cout << std::endl;
    }
}