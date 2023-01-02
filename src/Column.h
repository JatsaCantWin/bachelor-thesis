#ifndef PRIAMUS_COLUMN_H
#define PRIAMUS_COLUMN_H

#include <vector>
#include "Cell.h"

class Column {
private:
    std::vector<Cell> cellContainer;
    std::string columnName;
public:
    explicit Column(const std::string& columnName);

    const std::vector<Cell>& getCells() const;
    void addCell(const Cell& newCell);
    std::string getName() const;
};


#endif //PRIAMUS_COLUMN_H
