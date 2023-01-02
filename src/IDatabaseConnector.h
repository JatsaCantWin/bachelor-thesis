#ifndef PRIAMUS_IDATABASECONNECTOR_H
#define PRIAMUS_IDATABASECONNECTOR_H


#include "../DataContainers/Table.h"

class IDatabaseConnector {
private:
    virtual Table getTable(const std::string& tableName) = 0;
    virtual std::vector<std::string> getTableNames() = 0;
public:
    virtual int insertTables(const std::vector<Table>& tableList) = 0;
    virtual std::vector<Table> getAllTables() = 0;
};



#endif //PRIAMUS_IDATABASECONNECTOR_H
