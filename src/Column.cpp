#include "Column.h"

Column::Column(const std::string& columnName) {
    this->columnName = columnName;
}

const std::vector<Cell> &Column::getCells() const {
    return cellContainer;
}

std::string Column::getName() const {
    return columnName;
}

void Column::addCell(const Cell& newCell) {
    cellContainer.push_back(newCell);
}