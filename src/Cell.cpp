#include "Cell.h"

#include <regex>

Cell::Cell(const std::string& dataString, DataType dataType) {
    this->dataString = dataString;
    this->dataType = dataType;
}

Cell Cell::createFromSQLite(const std::string& dataString, const std::string& sqliteDataTypeString) {
    DataType dataType;

    if (std::regex_match(sqliteDataTypeString, std::regex("INTEGER")))
        dataType = DataType::Integer;
    else if (std::regex_match(sqliteDataTypeString, std::regex("NUMERIC(.*)")))
        dataType = DataType::FloatingPoint;
    else
        dataType = DataType::String;

    return Cell{dataString, dataType};
}

std::string Cell::getData() const {
    return dataString;
}

std::string Cell::getDataCassandra() const {
    if (dataType == DataType::String) {
        std::string bracketedData;
        for (auto character : dataString) {
            if (character == '\'') {
                bracketedData += "\'";
            }
            bracketedData += character;
        }
        return "'" + bracketedData + "'";
    }
    return dataString;
}

std::string Cell::getDataTypeCassandra() const {
    switch (dataType) {
        case DataType::Integer:
            return "int";
        case DataType::FloatingPoint:
            return "float";
        case DataType::String:
            return "text";
    }
}


