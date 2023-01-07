
#include "rm.h"

#include <iostream>
#include <string>

#include <stdlib.h> 
#include <stdexcept>
#include <stdio.h> 

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    if(RM_ScanIterator::rbfmScanIterator.getNextRecord(rid,data) == 0){
        return 0;
    }
    return RM_EOF;
}

RC RM_ScanIterator::close() {
    if(RM_ScanIterator::rbfmScanIterator.close()==0){
        return 0;
    }
    return -1;
}

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}


RelationManager::RelationManager()
{
    
}

RelationManager::~RelationManager()
{
}

void createTablesDescriptor(vector<Attribute> &tablesDescriptor)
{
    /*
    Tables (table-id:int, table-name:varchar(50), file-name:varchar(50), system-table-flag: int)
    */
    tablesDescriptor.clear();
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    tablesDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tablesDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tablesDescriptor.push_back(attr);

    attr.name = "system-table-flag"; // 1 for system table, 0 for user table.
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    tablesDescriptor.push_back(attr);
}

void prepareTablesTuple(void *data, int tableID, const string tableName, const string fileName, int systemTableFlag)
{
    /*
    Format of Tables:
    Tables (table-id:int, table-name:varchar(50), file-name:varchar(50), system-table-flag: int)
    */
    unsigned char *nullsIndicator = (unsigned char *) malloc(1);
    memset(nullsIndicator, 0, 1);
    nullsIndicator[0] = 0;
    
    int offset = 0;
    int nullFieldsIndicatorActualSize = 1; //less than 8 fields, need 1 byte of NULL indication

    // Null-indicator for the fields (00000000)
    memcpy((char *)data + offset, nullsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // Beginning of the actual data    
    //append tableID data
    memcpy((char *)data + offset, &tableID, sizeof(int));
    offset += sizeof(int);

    //append tableName data. First add length of the string tableName and then actual string data.
    int tableNameLength = strlen(tableName.c_str());
    memcpy((char *)data + offset, &tableNameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data + offset, tableName.c_str(), tableNameLength);
    offset += tableNameLength;

    //append fileName data. First add length of the string fileName and then actual string data.
    int fileNameLength = strlen(fileName.c_str());
    memcpy((char *)data + offset, &fileNameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data + offset, fileName.c_str(), fileNameLength);
    offset += fileNameLength;

    //append system-table-flag data
    memcpy((char *)data + offset, &systemTableFlag, sizeof(int));
    offset += sizeof(int);

    free(nullsIndicator);
}

void createColumnsDescriptor(vector<Attribute> &columnsDescriptor)
{
    /*
    Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
    */

    columnsDescriptor.clear();

    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    columnsDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);
}

void prepareColumnsTuple(void *data, int tableID, const string columnName, int columnType, int columnLength, int columnPosition)
{
    /*
    Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
    */
   
    unsigned char *nullsIndicator = (unsigned char *) malloc(1);
    memset(nullsIndicator, 0, 1);
    nullsIndicator[0] = 0;
    
    int offset = 0;
    int nullFieldsIndicatorActualSize = 1; //less than 8 fields, need 1 byte of NULL indication

    // Null-indicator for the fields (00000000)
    memcpy((char *)data + offset, nullsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // Beginning of the actual data    
    //append tableID data
    memcpy((char *)data + offset, &tableID, sizeof(int));
    offset += sizeof(int);

    //append columnName data. First add length of the string tableName and then actual string data.
    int columnNameLength = strlen(columnName.c_str());
    memcpy((char *)data + offset, &columnNameLength, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data + offset, columnName.c_str(), columnNameLength);
    offset += columnNameLength;

    //append column type data
    memcpy((char *)data + offset, &columnType, sizeof(int));
    offset += sizeof(int);

    //append column length data
    memcpy((char *)data + offset, &columnLength, sizeof(int));
    offset += sizeof(int);

    //append column position data
    memcpy((char *)data + offset, &columnPosition, sizeof(int));
    offset += sizeof(int);

    free(nullsIndicator);
}


RC RelationManager::createCatalog()
{
    /*
    Catalog format:
    Tables (table-id:int, table-name:varchar(50), file-name:varchar(50), system-table-flag:int)
    Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
    */
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    if(rbfm->createFile("Tables")==0 && rbfm->createFile("Columns")==0)
    {
        RC rc;
        FileHandle fileHandle;
        RID rid;
        void *data = malloc(4095);

        //Tables catalog part        
        if(rbfm->openFile("Tables", fileHandle)!=0)
        {
            rbfm->closeFile(fileHandle);
            return -1;
        }
        vector<Attribute> TablesDescriptor;
        createTablesDescriptor(TablesDescriptor);
        
        //Load the data variable with required data as per the mentioned API format
        //Here, the required data to to stored on data variable is (1, "Tables", "Tables", 1)
	    prepareTablesTuple(data,1,"Tables","Tables",1);
        //Write the data in file using insertRecord method.
        rc = rbfm->insertRecord(fileHandle, TablesDescriptor, data, rid);
        assert(rc==0 && "1st insert successful");

        //Load (2, "Columns", "Columns", 1) in the data variable.
	    prepareTablesTuple(data,2,"Columns","Columns",1);
        //Write the data in file using insertRecord method.
        rc = rbfm->insertRecord(fileHandle, TablesDescriptor, data, rid);
        assert(rc==0 && "2nd insert successful");

        //close the file
        rc = rbfm->closeFile(fileHandle);
        assert(rc==0 && "close file successful");
        
        free(data);
        
        /*
        **************************************************
        */

        //Columns catalog part
        if(rbfm->openFile("Columns", fileHandle))
        {
            rbfm->closeFile(fileHandle);
            return -1;
        }

        vector<Attribute> columnsDescriptor;
        createColumnsDescriptor(columnsDescriptor);
        data = malloc(4095);
        
        //Load (1, "table-id", TypeInt, 4 , 1) on data
	    prepareColumnsTuple(data,1,"table-id",TypeInt,4,1);
        //Write the data in file using insertRecord method.
        rc = rbfm->insertRecord(fileHandle, TablesDescriptor, data, rid);
        assert(rc==0 && "prepare columns insert 1");

        //Load (1, "table-name", TypeVarChar, 50, 2) on data
	    prepareColumnsTuple(data,1,"table-name",TypeVarChar,50,2);
        //Write the data in file using insertRecord method.
        rc = rbfm->insertRecord(fileHandle, TablesDescriptor, data, rid);
        assert(rc==0 && "prepare columns insert 2");

        //Load (1, "file-name", TypeVarChar, 50, 3) on data
	    prepareColumnsTuple(data,1,"file-name",TypeVarChar,50,3);
        //Write the data in file using insertRecord method.
        rc = rbfm->insertRecord(fileHandle, TablesDescriptor, data, rid);
        assert(rc==0 && "prepare columns insert 3");

        /*
        attr.name = "system-table-flag"; // 1 for system table, 0 for user table.
        attr.type = TypeInt;
        attr.length = (AttrLength)4;
        tablesDescriptor.push_back(attr);        
        */        

        //Load (1, "system-table-flag", TypeInt, 4, 4) on data
	    prepareColumnsTuple(data,1,"system-table-flag",TypeInt,4,4);
        //Write the data in file using insertRecord method.
        rc = rbfm->insertRecord(fileHandle, TablesDescriptor, data, rid);
        assert(rc==0 && "prepare columns insert 4");

        //Load (2, "table-id", TypeInt, 4, 1) on data
	    prepareColumnsTuple(data,2,"table-id",TypeInt,4,1);
        //Write the data in file using insertRecord method.
        rc = rbfm->insertRecord(fileHandle, TablesDescriptor, data, rid);
        assert(rc==0 && "prepare columns insert 5");
        
        //Load (2, "column-name",  TypeVarChar, 50, 2) on data
	    prepareColumnsTuple(data,2,"column-name",TypeVarChar,50,2);
        //Write the data in file using insertRecord method.
        rc = rbfm->insertRecord(fileHandle, TablesDescriptor, data, rid);
        assert(rc==0 && "prepare columns insert 6");
        
        //Load (2, "column-type", TypeInt, 4, 3) on data
	    prepareColumnsTuple(data,2,"column-type",TypeInt,4,3);
        //Write the data in file using insertRecord method.
        rc = rbfm->insertRecord(fileHandle, TablesDescriptor, data, rid);
        assert(rc==0 && "prepare columns insert 7");
        
        //Load (2, "column-length", TypeInt, 4, 4) on data
	    prepareColumnsTuple(data,2,"column-length",TypeInt,4,4);
        //Write the data in file using insertRecord method.
        rc = rbfm->insertRecord(fileHandle, TablesDescriptor, data, rid);
        assert(rc==0 && "prepare columns insert 8");
        
        //Load (2, "column-position", TypeInt, 4, 5) on data
	    prepareColumnsTuple(data,2,"column-position",TypeInt,4,5);
        //Write the data in file using insertRecord method.
        rc = rbfm->insertRecord(fileHandle, TablesDescriptor, data, rid);
        assert(rc==0 && "prepare columns insert 9");
    
        //free data variable
        free(data);
        //close the file
        rc = rbfm->closeFile(fileHandle);
        assert(rc==0 && "close file successful");
        return rc;
    }
    
    cout<<"Catalog Tables already exist. Please delete the existing file and create again. Thanks.";
    return -1;
}

RC RelationManager::deleteCatalog()
{
    RC rc;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RC temp1 = rbfm->destroyFile("Tables");
    RC temp2 = rbfm->destroyFile("Columns");
	return 0;    
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;

    if (rbfm->createFile(tableName) != 0)
    {
        cout<<"The file "<<tableName<<" aready exists in database. Delete that file then create again. I am not allowed to overwrite, sorry haha\n";
        return -1;
    }

    int32_t id;

    if(rbfm->openFile("Tables", fileHandle) != 0)
    {
        return -1;
    }

    vector<string> columns;
    columns.push_back("table-id");

    RBFM_ScanIterator iter;
    
    vector<Attribute> TablesDescriptor;
    createTablesDescriptor(TablesDescriptor);
    if(rbfm->scan(fileHandle,TablesDescriptor, "table-id", NO_OP, NULL, columns, iter) != 0) // pull out all records that don't have table-id as NULL
    {
        return -1;
    }

    //Find a new (appropriate/available) table ID for the record to be entered.
    RID rid;
    void *data = malloc (4095); //reserve space for null byte + 4 bytes (int size).
    int32_t max_table_id = 0;
    while(iter.getNextRecord(rid, data) == 0) //read next pair of rid and data
    {
        int32_t tid;
        char nullByte = 0;
        memcpy(&nullByte, data, 1); //nullbyte variable with null byte data
        memcpy(&tid, (char*) data + 1, 4); //read next 4 bytes in tid as table-id
        if (tid > max_table_id) 
            max_table_id = tid; //update max_table_id
    }

    free(data);
    id = max_table_id + 1;
    rbfm->closeFile(fileHandle);
    iter.close();

    RC rc;

    //insert in Tables file
    if(rc = rbfm->openFile("Tables", fileHandle)!=0)
        return -1;
    //vector<Attribute> TablesDescriptor;
    //createTablesDescriptor(TablesDescriptor);
    void *tuple = malloc(4095);
    const string fileName = tableName + ".tbl";
    prepareTablesTuple(tuple,id,tableName,tableName,0);
    rbfm->insertRecord(fileHandle, TablesDescriptor, tuple, rid);
    rbfm->closeFile(fileHandle);

    //insert in Columns file
    if(rc = rbfm->openFile("Columns", fileHandle)!=0)
        return -1;
    vector<Attribute> columnsDescriptor;
    createColumnsDescriptor(columnsDescriptor);

    for (size_t i = 0; i < attrs.size(); i++)
    {
        prepareColumnsTuple(tuple,id,attrs[i].name,attrs[i].type,attrs[i].length,i+1);
        rbfm->insertRecord(fileHandle, columnsDescriptor, tuple, rid);
        //cout<<"rid:"<<rid.pageNum<<","<<rid.slotNum<<","<<attrs[i].name<<"\n";
    }

    free(tuple);
    rbfm->closeFile(fileHandle);
    iter.close();
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    // don't forget to add checks for system table. system table should not be deleted
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    int32_t tid;

    if (rbfm->destroyFile(tableName) != 0)
    {
        return -1;
    }

    if(rbfm->openFile("Tables", fileHandle) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    vector<string> columns;
    columns.push_back("table-id");

    void *value = malloc(4095); //reserve space for table-name:varchar(50)
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, 4);
    memcpy((char*)value+4, tableName.c_str(), name_len);

    RBFM_ScanIterator iter;
    vector<Attribute> TablesDescriptor;
    createTablesDescriptor(TablesDescriptor);

    if (rbfm->scan(fileHandle, TablesDescriptor, "table-name", EQ_OP, value, columns, iter) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    free(value);
    
    //Find the table ID for the record to be deleted.
    RID rid;
    void *data = malloc (4095); //reserve space for null byte + 4 bytes (int size).
    if(iter.getNextRecord(rid, data) == 0) //read next pair of rid and data
    {
        int32_t tid_temp;
        char nullByte = 0;
        memcpy(&nullByte, data, 1); //nullbyte variable with null byte data
        memcpy(&tid_temp, (char*) data + 1, 4); //read next 4 bytes in tid as table-id
        tid = tid_temp; // assign table_id
    }
    else
        return -1;
    free(data);
    iter.close();

    //Delete record from Tables file
    rbfm->deleteRecord(fileHandle, TablesDescriptor, rid);
    rbfm->closeFile(fileHandle);

    //repeat same for Columns file
    if (rbfm->openFile("Columns", fileHandle) != 0)
        return -1;
    vector<Attribute> columnsDescriptor;
    createColumnsDescriptor(columnsDescriptor);
    value = malloc(4095);
    memcpy(value,&tid,4);
//    value = &tid;
    RBFM_ScanIterator iter2;
    rbfm->scan(fileHandle, columnsDescriptor, "table-id", EQ_OP, value, columns, iter2);

    data = malloc (4095);
    while(iter2.getNextRecord(rid, data) == 0) //get pairs of rid and data
    {
        if(rbfm->deleteRecord(fileHandle, columnsDescriptor, rid) != 0) //delete record from Columns file with above found rid
        {
            rbfm->closeFile(fileHandle);
            return -1;
        }
    }
    free(value);
//    value = NULL;
    free(data);
    rbfm->closeFile(fileHandle);
    iter.close();
    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    int32_t tid;

    if(rbfm->openFile("Tables", fileHandle) != 0)
    {
        return -1;
    }

    vector<string> columns;
    columns.push_back("table-id");

    void *value = malloc(4095); //reserve space for table-name:varchar(50)
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, 4);
    memcpy((char*)value+4, tableName.c_str(), name_len);

    RBFM_ScanIterator iter;
    vector<Attribute> TablesDescriptor;
    createTablesDescriptor(TablesDescriptor);

    if (rbfm->scan(fileHandle, TablesDescriptor, "table-name", EQ_OP, value, columns, iter) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    free(value);
    
    //Find the table ID for the record to be deleted.
    RID rid;
    void *data = malloc (4095); //reserve space for null byte + 4 bytes (int size).
    if(iter.getNextRecord(rid, data) == 0) //read next pair of rid and data
    {
        int32_t tid_temp;
        char nullByte = 0;
        memcpy(&nullByte, data, 1); //nullbyte variable with null byte data
        memcpy(&tid_temp, (char*) data + 1, 4); //read next 4 bytes in tid as table-id
        tid = tid_temp; // assign table_id
    }
    else
        return -1;
    free(data);
    iter.close();
    rbfm->closeFile(fileHandle);
    //cout<<"hi"<<tid<<"\n";   

    /************** table-id found ****************/

    //using tid, pull the whole record this time, not just table-id as above block

    //repeat same for Columns file
    if (rbfm->openFile("Columns", fileHandle) != 0)
        return -1;

    vector<Attribute> columnsDescriptor; //for scan
    createColumnsDescriptor(columnsDescriptor);

    vector<Attribute> columnsDescriptor2; // after scan
    //createColumnsDescriptor(columnsDescriptor);
    Attribute attr_projection;
    attr_projection.name = "column-name";
    attr_projection.type = TypeVarChar;
    attr_projection.length = (AttrLength)50;
    columnsDescriptor2.push_back(attr_projection);


    attr_projection.name = "column-type";
    attr_projection.type = TypeInt;
    attr_projection.length = (AttrLength)4;
    columnsDescriptor2.push_back(attr_projection);

    attr_projection.name = "column-length";
    attr_projection.type = TypeInt;
    attr_projection.length = (AttrLength)4;
    columnsDescriptor2.push_back(attr_projection);

    value = malloc(4095);
    memcpy(value,&tid,4);

    vector<string> columns2;
    columns2.push_back("column-name");
    columns2.push_back("column-type");
    columns2.push_back("column-length");

    RBFM_ScanIterator iter2;
    rbfm->scan(fileHandle, columnsDescriptor, "table-id", EQ_OP, value, columns2, iter2);

    data = malloc (4095);
    while(iter2.getNextRecord(rid, data) == 0) //get pairs of rid and data
    {
        //rbfm->printRecord(columnsDescriptor2,data);
        Attribute attr;
        // | 1 NIA | 4 bytes varlen| varchar | 4 varlen | varchar | 4 byte int|
        int offset = 1;
        int strlength = 0;
        memcpy(&strlength, (char *)data + offset, sizeof(int));
        char *attributeName = (char *)malloc(strlength);
        memset(attributeName, 0, strlength);
        memcpy(attributeName, (char *)data + offset + sizeof(int), strlength);
        offset += sizeof(int) + strlength;

        int attributeType = 0;
        memcpy(&attributeType, (char *)data + offset, sizeof(int));
        offset += sizeof(int);

        int attributeLength = 0;
        memcpy(&attributeLength, (char *)data + offset, sizeof(int));
        offset += sizeof(int);

        attr.name.assign(attributeName, (unsigned)strlength);
        //		attr.name = attributeName;
        switch (attributeType) {
        case TypeInt:
            attr.type = TypeInt;
            break;
        case TypeReal:
            attr.type = TypeReal;
            break;
        case TypeVarChar:
            attr.type = TypeVarChar;
            break;
        }
        attr.length = (AttrLength)attributeLength;

        attrs.push_back(attr);
        free(attributeName);
    }
    free(value);
//    value = NULL;
    free(data);
    rbfm->closeFile(fileHandle);
    iter.close();
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    if(rbfm->openFile(tableName,fileHandle)!=0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    vector<Attribute>attrs;
    if(getAttributes(tableName,attrs)!=0) //get attributes of the record to be inserted
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    if(rbfm->insertRecord(fileHandle,attrs,data,rid))
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    if(rbfm->openFile(tableName,fileHandle)!=0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    vector<Attribute>attrs;
    if(getAttributes(tableName,attrs)!=0) //get attributes of the record to be inserted
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    if(rbfm->deleteRecord(fileHandle,attrs,rid))
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    if(rbfm->openFile(tableName,fileHandle)!=0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    vector<Attribute>attrs;
    if(getAttributes(tableName,attrs)!=0) //get attributes of the record to be inserted
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    if(rbfm->readRecord(fileHandle,attrs,rid,data))
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    if (rbfm->openFile(tableName, fileHandle) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    vector<Attribute> attrs;
    if (getAttributes(tableName, attrs) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    if (rbfm->readAttribute(fileHandle,attrs,rid,attributeName,data) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    rbfm->closeFile(fileHandle);
    return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	return -1;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	return -1;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	return -1;
}

RC RelationManager::scan(const string &tableName,
        const string &conditionAttribute,
        const CompOp compOp,                  // comparison type such as "<" and "="
        const void *value,                    // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RM_ScanIterator &rm_ScanIterator){

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    if (rbfm->openFile(tableName, fileHandle) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    vector<Attribute> attrs;
    if (this->getAttributes(tableName, attrs) != 0)
    {
        rbfm->closeFile(fileHandle);
        return -1;
    }
    if(rbfm->scan(fileHandle,attrs,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.rbfmScanIterator) != 0){
        return -1;
    }
    return 0;

}
