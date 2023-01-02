#ifndef PRIAMUS_TABLE_H
#define PRIAMUS_TABLE_H

#include <vector>
#include "Column.h"

class Table {
private:
    std::vector<Column> columnContainer;
    std::string tableName;
public:
    explicit Table(const std::string& tableName);

    const std::vector<Column>& getColumns() const;
    std::string getDataCassandra(int column, int row) const;
    std::string getData(int column, int row) const;
    std::string getName() const;
    void addColumn(const Column& newColumn);
    void addCell(int column, const Cell& newCell);
    void print();
};


#endif //PRIAMUS_TABLE_H
