
#include "qe.h"
#include <unordered_map>

Filter::Filter(Iterator* input, const Condition &condition) 
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RelationManager *rm = RelationManager::instance();
    RM_ScanIterator *iter = new RM_ScanIterator();
    FileHandle fileHandle;

    this->iter = iter;


    char arr[condition.lhsAttr.length()];
    strcpy(arr, condition.lhsAttr.c_str());
    string tableName = "";
    string conditionAttribute="";
    int flag =0;
    for(int i=0;i<sizeof(arr);i++)
    {
        if(arr[i]=='.')
        {    
            flag=1;
            continue;
        }
        if(flag==0)
            tableName+=arr[i];
        else
            conditionAttribute+=arr[i];

    }
    vector<Attribute> attrs;
    int openFileSuccess = rbfm->openFile(tableName, fileHandle);
    int getAttributeSucess = rm->getAttributes(tableName, attrs);
    vector<string> attributeNames;
    for(int i=0;i<attrs.size();i++)
    {
        attributeNames.push_back(attrs[i].name);
    }

    if(openFileSuccess==0 && getAttributeSucess==0)
    {
        rm->scan(tableName,conditionAttribute,condition.op,condition.rhsValue.data,attributeNames, *(this->iter));
    }
}

Project::Project(Iterator *input, const vector<string> &attrNames)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RelationManager *rm = RelationManager::instance();
    RBFM_ScanIterator iter;
    FileHandle fileHandle;
    string rawValue = attrNames.front();
    char arr[rawValue.length()];
    strcpy(arr, rawValue.c_str());
    string tableName = "";
    int attrNamesSize = attrNames.size();
    for(int i=0;i<sizeof(arr);i++)
    {
        if(arr[i]=='.')
        {   
            break;
        }
        tableName+=arr[i];
    }
    vector<string> attributes;
    int flag;
    for(int i=0;i<attrNamesSize;i++)
    {
        string attrName = attrNames.at(i);
        char arr[attrName.length()];
        strcpy(arr,attrName.c_str());
        flag = 0;
        string attr="";
        for(int i=0;i<sizeof(arr);i++)
        {
            if(arr[i]=='.')
            {   
                flag = 1;
                continue;
            }
            if(flag)
                attr+=arr[i];
        }
        attributes.push_back(attr);         
    }
    vector<Attribute> attrs;
    int openFileSuccess = rbfm->openFile(tableName, fileHandle);
    int getAttributeSucess = rm->getAttributes(tableName, attrs);
    if(openFileSuccess==0 && getAttributeSucess==0)
    {
        rbfm->scan(fileHandle, attrs, NULL ,NO_OP,NULL, attributes , iter);
    }
}

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RelationManager *rm = RelationManager::instance();
    FileHandle fileHandle;
    RM_ScanIterator iter1;
    RM_ScanIterator iter2;
    
    char arr[condition.lhsAttr.length()];
    strcpy(arr, condition.lhsAttr.c_str());
    string leftTableName = "";
    string leftConditionAttribute="";

    int flag = 0;
    for(int i=0;i<sizeof(arr);i++)
    {
        if(arr[i]=='.')
        {    
            flag=1;
            continue;
        }
        if(flag==0)
            leftTableName+=arr[i];
        else
            leftConditionAttribute+=arr[i];

    }

    char arr1[condition.rhsAttr.length()];
    strcpy(arr1, condition.rhsAttr.c_str());
    string rightTableName = "";
    string rightConditionAttribute="";
    flag = 0;
    for(int i=0;i<sizeof(arr1);i++)
    {
        if(arr1[i]=='.')
        {    
            flag=1;
            continue;
        }
        if(flag==0)
            rightTableName+=arr1[i];
        else
            rightConditionAttribute+=arr1[i];

    }

    cout<<leftTableName<<" ^^^^ "<<leftConditionAttribute<<"\n";
    cout<<rightTableName<<" $$$$ "<<rightConditionAttribute;
    vector<Attribute> leftAttrs;
    int openLeftFileSuccess = rbfm->openFile(leftTableName, fileHandle);
    //int getLeftAttributeSucess = rm->getAttributes(leftTableName, leftAttrs);
    vector<string> leftAttributeName;
    vector<string> rightAttributeName;
    
    if(openLeftFileSuccess == 0)
    {
        rm->scan(leftTableName, leftConditionAttribute, NO_OP, NULL, leftAttributeName , iter1);
    }

    RID rid;
    void *data = malloc (4095); 
    int32_t max_table_id = 0;
    while(iter1.getNextTuple(rid, data) == 0) //read next pair of rid and data
    {
        //   
    }
    


    // After Left table operations are done
    vector<Attribute> rightAttrs;
    int openRightFileSuccess = rbfm->openFile(rightTableName, fileHandle);
    //int getrightAttributeSucess = rm->getAttributes(rightTableName, rightAttrs);
    if(openRightFileSuccess == 0)
    {
        rm->scan(rightTableName, rightConditionAttribute, NO_OP, NULL, rightAttributeName , iter2);
    }
}
