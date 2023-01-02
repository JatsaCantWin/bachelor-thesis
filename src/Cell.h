#ifndef PRIAMUS_CELL_H
#define PRIAMUS_CELL_H

#include <string>

class Cell {
private:
    enum class DataType {Integer, FloatingPoint, String} dataType;
    std::string dataString;
    Cell(const std::string& dataString, DataType dataType);
public:
    static Cell createFromSQLite(const std::string& dataString, const std::string& sqliteDataTypeString);
    std::string getData() const;
    std::string getDataCassandra() const;
    std::string getDataTypeCassandra() const;
};


#endif //PRIAMUS_CELL_H
