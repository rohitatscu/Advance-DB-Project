#include "rbfm.h"
#include "math.h"



RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
    if(this->scanRID.size() > 0 && idx < this->scanRID.size()){
        RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
        void* dataRead = malloc(sizeOfRIDRecord[idx]); //1
        rbfm->readRecord(this->filehandler,this->recordDescriptor,scanRID[idx],dataRead);
        rid.pageNum = scanRID[idx].pageNum;
        rid.slotNum = scanRID[idx].slotNum;
        int offsetNew = 0;
        char* charDataPointer = (char*)dataRead;
        int sizeOfRecordDescriptor = this->recordDescriptor.size();
        int sizeOfAttrDescriptor = this->attributeNames.size();
        int leftmostFlagByteSize;
        int leftmostFlagProjectByteSize;
        if(sizeOfRecordDescriptor%8==0)
        {
            leftmostFlagByteSize = sizeOfRecordDescriptor/8;
        }
        else
            leftmostFlagByteSize = 1 + sizeOfRecordDescriptor/8;

        if(sizeOfAttrDescriptor%8==0){
            leftmostFlagProjectByteSize = sizeOfAttrDescriptor/8;
        }
        else{
            leftmostFlagProjectByteSize = 1 + sizeOfAttrDescriptor/8;
        }
        charDataPointer+=leftmostFlagByteSize;

        int nullFlagIdx = 0;
        int bitChecker = 7;

        int nullFlagProjIdx = 0;
        int projBitChecker = 7;

        int nullOffset = leftmostFlagProjectByteSize;
        int projectedRecordOffset = 0;
        for(vector<Attribute>::const_iterator recordDescriptor_itr = this->recordDescriptor.begin(); recordDescriptor_itr != this->recordDescriptor.end(); recordDescriptor_itr++)
        {
            bool attrFound = false;
            bool isNull = *((char *) dataRead + nullFlagIdx) & 1 << bitChecker;
            if (bitChecker == 0) {
                nullFlagIdx++;
                bitChecker = 7;
            } else {
                bitChecker--;
            }
            Attribute attr = *recordDescriptor_itr;
            for(vector<string>::const_iterator attrNamesDescriptor_itr = this->attributeNames.begin(); attrNamesDescriptor_itr != this->attributeNames.end(); attrNamesDescriptor_itr++) {
                string attribute = *attrNamesDescriptor_itr;

                if (attr.name == attribute) {
                    attrFound = true;
                    if (isNull) {
                        *((char *) data + nullFlagProjIdx) = *((char *) data + nullFlagProjIdx) | 1 << projBitChecker;
                    }
                    if (projBitChecker == 0) {
                        nullFlagProjIdx++;
                        projBitChecker = 7;
                    } else {
                        projBitChecker--;
                    }
                    break;
                }
            }

                if (attr.type == TypeInt && !isNull) {
                    if(attrFound){
                        memcpy((char*)data+nullOffset+projectedRecordOffset,charDataPointer,4);
                        projectedRecordOffset += 4;
                    }
                    charDataPointer += 4;

                }

                if (attr.type == TypeReal && !isNull) {
                    if(attrFound){
                        memcpy((char*)data+nullOffset+projectedRecordOffset,charDataPointer,4);
                        projectedRecordOffset += 4;
                    }
                    charDataPointer += 4;
                }

                if (attr.type == TypeVarChar && !isNull) {
                    int *size = (int *) charDataPointer; //type cast

                    if(attrFound){
                        memcpy((char*)data+nullOffset+projectedRecordOffset,charDataPointer,4);
                        projectedRecordOffset += 4;
                        memcpy((char*)data+nullOffset+projectedRecordOffset,charDataPointer+projectedRecordOffset,*size);
                        projectedRecordOffset += *size;
                    }
                    charDataPointer += 4;
                    charDataPointer += *size;
                }

        }
        this->idx++;
        free(dataRead);//1
        return 0;
    }
    return RBFM_EOF;
}



RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    PagedFileManager* pfm = PagedFileManager::instance();
    return pfm->createFile(fileName.c_str());
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    PagedFileManager* pfm = PagedFileManager::instance();
	return pfm->destroyFile(fileName.c_str());
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    PagedFileManager* pfm = PagedFileManager::instance();
	return pfm->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    PagedFileManager* pfm = PagedFileManager::instance();
	return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    int size_of_written_data = RecordBasedFileManager::findSizeOfData((char*)data,recordDescriptor);
//    int size_of_written_data = recordDescriptor.size();
    if(!RecordBasedFileManager::isLastPageFree(fileHandle,size_of_written_data)){
        void* dataWrite = malloc(PAGE_SIZE);//2
        char* tempP = (char *)dataWrite;
        int i = 0;
        while(i<PAGE_SIZE) {
            *((char *)tempP+i) = 3;
            i++;
        }
        RecordBasedFileManager::initalizeRecordPageHeader(dataWrite);
        fileHandle.appendPage(dataWrite);
//        fflush(fileHandle.filePointer);
        if(ferror(fileHandle.filePointer)){
            cout << "ERROR APPENDING PAGE"<<"\n";
        }
        if(dataWrite != NULL){
            free(dataWrite);//2
            dataWrite = NULL;
        }
    }

        int PageNumber = fileHandle.getNumberOfPages();
        rid.pageNum = (unsigned)PageNumber-1;
        void* dataRead = malloc(PAGE_SIZE);//3
        int ReadingResult = fileHandle.readPage(PageNumber-1,dataRead);
        if(ferror (fileHandle.filePointer)){
            cout << "ERROR READING PAGE"<<"\n";
        }
        int offset = RecordBasedFileManager::getNewRecordSpaceLocation(dataRead);
        if(ReadingResult == -1){
            "Create page or page read failed";
            if(dataRead != NULL){
                free(dataRead); //3
                dataRead = NULL;
            }
            rid.pageNum = (unsigned )0;
            rid.slotNum = (unsigned )0;
            return -1;
        }
        memcpy((char*)dataRead + offset,data,size_of_written_data);
        RecordBasedFileManager::addRecordToSlotHeader(dataRead,size_of_written_data,offset, rid);
        fileHandle.writePage(rid.pageNum,dataRead);
//        fflush(fileHandle.filePointer);
        if(ferror (fileHandle.filePointer)){
            cout << "ERROR WRITING PAGE"<<"\n";
        }
        if(dataRead != NULL){
            free(dataRead);//3
            dataRead = NULL;
        }
        return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    char *content = (char*)malloc(PAGE_SIZE);//4
    int rp = fileHandle.readPage(rid.pageNum, content);
    if(ferror (fileHandle.filePointer)){
        cout << "ERROR READING PAGE"<<"\n";
    }
    if(rp == -1)
    {
        perror("readRecord");
        fprintf(stderr, "Page %d does not exist.\n", rid.pageNum);
        return -1;
    }
    if(rid.slotNum < 1 || rid.pageNum < 0){
        cout << endl << "SLOT or PAGE DOESNT EXIST" << endl;
    }
    int slotOffset = PAGE_SIZE - (8 + rid.slotNum * SLOT_SIZE);
    int* sizePtr = (int *)(content+slotOffset+4);
    int* offsetPtr = (int *)(content+slotOffset);
    int recordOffset = *offsetPtr;
    int sizeOfRecord = *sizePtr;
    if(recordOffset == -1 || sizeOfRecord == -1){
        //cout << "This record has been deleted." << rid.pageNum << " :PAGE NUM " << rid.slotNum << " :SLOT NUM " << "\n";
        return -1;
    }
    void* contentRecord = (void*)(content+recordOffset);
    memcpy(data,(char*)(content+recordOffset),sizeOfRecord);
//    data = (void*)dataRecord;
    free(content);//4
    return 0;
}

RC RecordBasedFileManager::sizeOfRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    char *content = (char*)malloc(PAGE_SIZE);//5
    int rp = fileHandle.readPage(rid.pageNum, content);
    if(ferror (fileHandle.filePointer)){
        cout << "ERROR READING PAGE"<<"\n";
    }
    if(rp == -1)
    {
        perror("readRecord");
        fprintf(stderr, "Page %d does not exist.\n", rid.pageNum);
        return -1;
    }
    if(rid.slotNum < 1 || rid.pageNum < 0){
        cout << endl << "SLOT or PAGE DOESNT EXIST" << endl;
    }
    int slotOffset = PAGE_SIZE - (8 + rid.slotNum * SLOT_SIZE);
    int* sizePtr = (int *)(content+slotOffset+4);
    int* offsetPtr = (int *)(content+slotOffset);
    int recordOffset = *offsetPtr;
    int sizeOfRecord = *sizePtr;
    if(recordOffset == -1 || sizeOfRecord == -1){
        //cout << "This record has been deleted." << rid.pageNum << " :PAGE NUM " << rid.slotNum << " :SLOT NUM " << "\n";
        return -1;
    }
    free(content);//5
    return sizeOfRecord;
//    data = (void*)dataRecord;


}


RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
    char* charDataPointer = (char*)data;
    int sizeOfRecordDescriptor = recordDescriptor.size();
    int leftmostFlagByteSize;
    if(sizeOfRecordDescriptor%8==0)
    {
        leftmostFlagByteSize = sizeOfRecordDescriptor/8;
    }  
    else
        leftmostFlagByteSize = 1 + sizeOfRecordDescriptor/8;
    charDataPointer+=leftmostFlagByteSize;
    
    if(recordDescriptor.size() == 0)
    {
        return -1;
    }
    int nullFlagIdx = 0;
    int bitChecker  = 7;
    
    for(vector<Attribute>::const_iterator recordDescriptor_itr = recordDescriptor.begin(); recordDescriptor_itr != recordDescriptor.end(); recordDescriptor_itr++)
    {
        bool nullBit = *((char*)data + nullFlagIdx) & 1 << bitChecker;
        if(bitChecker == 0){
            nullFlagIdx++;
            bitChecker = 7;
        }
        else{
            bitChecker--;
        }
        Attribute attr = *recordDescriptor_itr;
        if(!nullBit){
            if(attr.type == TypeInt)
            {
                cout<<"attribute name: "<<attr.name<<", attribute type: int, attribute length: "<<attr.length<<", attribute data: "<<((*(int*)charDataPointer!=NULL) ? *(int*)charDataPointer : NULL)<<"\n";
                charDataPointer += 4;
            }

            if(attr.type == TypeReal)
            {
                cout<<"attribute name: "<<attr.name<<", attribute type: real, attribute length: "<<attr.length<<", attribute data: "<<((*(float*)charDataPointer!=NULL) ? *(float*)charDataPointer : NULL) <<"\n";
                charDataPointer += 4;
            }

            if(attr.type == TypeVarChar)
            {
                cout<<"attribute name: "<<attr.name<<", attribute type: varchar, attribute length: "<<attr.length<<", attribute data: ";
                int* size = (int*)charDataPointer; //type cast
                charDataPointer += 4;
                for(int size_itr = 0; size_itr < *size; size_itr++)
                {
                    char charDataToPrint;
                    if(charDataPointer != NULL)
                    {
                        charDataToPrint = *charDataPointer;
                        cout<<charDataToPrint;
                        charDataPointer++;
                    }
                }
                cout<<"\n";
            }
        }
        }

    
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    char *content = (char*)malloc(PAGE_SIZE);//6
    int rp = fileHandle.readPage(rid.pageNum, content);
    char* endOfPageData = (char*)(content) + PAGE_SIZE;
    int numberOfSlots = RecordBasedFileManager::getNumberOfSlotsForPagePointer(endOfPageData);
    int maxOffset = RecordBasedFileManager::getMaxOffsetFromSlot(numberOfSlots,endOfPageData);
    if(ferror (fileHandle.filePointer)){
        cout << "ERROR READING PAGE"<<"\n";
    }
    if(rp == -1)
    {
        perror("readRecord");
        fprintf(stderr, "Page %d does not exist.\n", rid.pageNum);
        return -1;
    }
    if(rid.slotNum < 1 || rid.pageNum < 0){
        cout << endl << "SLOT or PAGE DOESNT EXIST" << endl;
    }
    int pageSizeOffset = PAGE_SIZE - 4;
    int slotOffset = PAGE_SIZE - (8 + rid.slotNum * SLOT_SIZE);
    int* sizePtr = (int *)(content+slotOffset+4);
    int* offsetPtr = (int *)(content+slotOffset);
    int recordOffset = *offsetPtr;
    *offsetPtr = -1;
    int sizeOfRecord = *sizePtr;
    *sizePtr = -1;
    for(int slotNumIter = rid.slotNum+1;slotNumIter <=numberOfSlots;slotNumIter++){
        int newSlotOffset = PAGE_SIZE - (8 + slotNumIter * SLOT_SIZE);
        int* newOffsetPtr = (int *)(content+newSlotOffset);
        *newOffsetPtr = *newOffsetPtr - sizeOfRecord;
    }
    int* pageSizePtr = (int *)(content+pageSizeOffset);
    *pageSizePtr = *pageSizePtr + sizeOfRecord;

    void* contentRecordToOverwrite = (void*)(content+recordOffset);
    void* contentRecordAfterDelete = (void*)(content+recordOffset+sizeOfRecord);
    if(rid.slotNum < numberOfSlots){
        memcpy(contentRecordToOverwrite,contentRecordAfterDelete,maxOffset - (recordOffset + sizeOfRecord));
    }
    memset(content +  (maxOffset - sizeOfRecord), 1,sizeOfRecord );
    fileHandle.writePage(rid.pageNum,content);
//        fflush(fileHandle.filePointer);
    if(ferror (fileHandle.filePointer)){
        cout << "ERROR WRITING PAGE"<<"\n";
    }
    free(content);//6
    return 0;
}

RC RecordBasedFileManager :: findSizeOfData(char* data, const vector<Attribute> recordDescriptor){

    int offsetNew = 0;
    char* charDataPointer = (char*)data;
    int sizeOfRecordDescriptor = recordDescriptor.size();
    int leftmostFlagByteSize;
    if(sizeOfRecordDescriptor%8==0)
    {
        leftmostFlagByteSize = sizeOfRecordDescriptor/8;
    }
    else
        leftmostFlagByteSize = 1 + sizeOfRecordDescriptor/8;
    charDataPointer+=leftmostFlagByteSize;
    offsetNew += leftmostFlagByteSize;


    int nullFlagIdx = 0;
    int bitChecker  = 7;

    for(vector<Attribute>::const_iterator recordDescriptor_itr = recordDescriptor.begin(); recordDescriptor_itr != recordDescriptor.end(); recordDescriptor_itr++)
    {
        bool nullBit = *((char*)data + nullFlagIdx) & 1 << bitChecker;
        if(bitChecker == 0){
            nullFlagIdx++;
            bitChecker = 7;
        }
        else{
            bitChecker--;
        }
        Attribute attr = *recordDescriptor_itr;
        if(!nullBit) {
            if (attr.type == TypeInt) {
                charDataPointer += 4;
                offsetNew += 4;
            }

            if (attr.type == TypeReal) {
                charDataPointer += 4;
                offsetNew += 4;
            }

            if (attr.type == TypeVarChar) {
                int *size = (int *) charDataPointer; //type cast
                charDataPointer += 4;
                offsetNew += 4;
                for (int size_itr = 0; size_itr < *size; size_itr++) {
                    char charDataToPrint = *charDataPointer;
                    charDataPointer++;
                    offsetNew += 1;
                }
            }
        }
    }

    return offsetNew;
}


int RecordBasedFileManager :: findSizeUptoAnAttribute(char* data, const vector<Attribute> recordDescriptor,const string attribute, AttrType *typeAttr, bool* isNull){

    int offsetNew = 0;
    char* charDataPointer = (char*)data;
    int sizeOfRecordDescriptor = recordDescriptor.size();
    int leftmostFlagByteSize;
    if(sizeOfRecordDescriptor%8==0)
    {
        leftmostFlagByteSize = sizeOfRecordDescriptor/8;
    }
    else
        leftmostFlagByteSize = 1 + sizeOfRecordDescriptor/8;
    charDataPointer+=leftmostFlagByteSize;
    offsetNew += leftmostFlagByteSize;


    int nullFlagIdx = 0;
    int bitChecker = 0;


    for(vector<Attribute>::const_iterator recordDescriptor_itr = recordDescriptor.begin(); recordDescriptor_itr != recordDescriptor.end(); recordDescriptor_itr++)
    {
        Attribute attr = *recordDescriptor_itr;

        bool tempNull = *((char*)data + nullFlagIdx) & 1 << bitChecker;
        if(bitChecker == 0){
            nullFlagIdx++;
            bitChecker = 7;
        }
        else{
            bitChecker--;
        }
        if(tempNull){
            *isNull = true;
            return 0;
        }
        else{
            *isNull=false;
        }
        if(attr.name == attribute){
            *typeAttr = attr.type;
            return offsetNew;
        }

        if(attr.type == TypeInt)
        {
            charDataPointer += 4;
            offsetNew += 4;
        }

        if(attr.type == TypeReal)
        {
            charDataPointer += 4;
            offsetNew += 4;
        }

        if(attr.type == TypeVarChar)
        {
            int* size = (int*)charDataPointer; //type cast
            charDataPointer += 4;
            offsetNew += 4;
            for(int size_itr = 0; size_itr < *size; size_itr++)
            {
                char charDataToPrint = *charDataPointer;
                charDataPointer++;
                offsetNew += 1;
            }
        }
    }

    return offsetNew;
}



RC RecordBasedFileManager::getNewRecordSpaceLocation(void* dataRead){
    char* endOfPageData = (char*)(dataRead) + PAGE_SIZE;
    int numberOfSlots = RecordBasedFileManager::getNumberOfSlotsForPagePointer(endOfPageData);
    int offset = RecordBasedFileManager::getMaxOffsetFromSlot(numberOfSlots,endOfPageData);
    return  offset;
}

bool RecordBasedFileManager::isLastPageFree(FileHandle &fileHandle,int size_of_written_data){
    int pageNumber = fileHandle.getNumberOfPages();
    if(pageNumber == 0){
        return false;
    }
    else{
        void *buffer = malloc(PAGE_SIZE); //7
        int read = fileHandle.readPage(pageNumber-1, buffer);
        if(ferror (fileHandle.filePointer)){
            cout << "ERROR READING PAGE"<<"\n";
        }
//        cout << read << "AFTER READING" << "\n";
        if(read == -1){
            if(buffer != NULL){
                free(buffer);//7
                buffer = NULL;
            }
            "Create page or page read failed";
            return false;
        }
        char* temp = (char*)buffer;
        temp = temp + PAGE_SIZE - 4;
        int* slotInitPointer = (int*) temp;
        int size_available = *slotInitPointer;
        if(buffer != NULL){
            free(buffer); //7
            buffer = NULL;
            temp = NULL;
            slotInitPointer = NULL;
        }
//        if(temp != NULL){
//            free(temp);
//        }
        if((size_available - 8) > size_of_written_data){
            return true;
        }
        else{
            return false;
        }
    }
    return  true;
}

void RecordBasedFileManager::addRecordToSlotHeader(void* dataRead, int size_of_data, int offset, RID &rid){
//    char* startOfPageData = dataRead;
    char* endOfPageData = (char*)dataRead + PAGE_SIZE;
    int numberOfExistingSlots = RecordBasedFileManager::updateSlotsForPagePointer(endOfPageData,size_of_data);
    rid.slotNum = (unsigned )(numberOfExistingSlots + 1);
//    endOfPageData -= 8;
    int endOfPageSlotOffset = 8 + SLOT_SIZE * numberOfExistingSlots;
//    endOfPageData -= STARTING_OFFSET_SIZE;

    int* size_of_slot = (int*) (endOfPageData - endOfPageSlotOffset - STARTING_OFFSET_SIZE);
    *size_of_slot = size_of_data;
//    endOfPageData -= STARTING_OFFSET_SIZE;
    int* offset_of_slot = (int*) (endOfPageData - endOfPageSlotOffset - (2 * STARTING_OFFSET_SIZE));
    *offset_of_slot = offset;
}

RC RecordBasedFileManager :: getActualByteForNullsIndicator(int fieldCount) {
    return ceil((double) fieldCount / CHAR_BIT);
}

void RecordBasedFileManager :: initalizeRecordPageHeader(void *dataWrite) {
    char* tempP = (char *)dataWrite;
    int* slotsPtr = (int *)(tempP+4096-8);
    int* sizePtr = (int *)(tempP+4096-4);
    *slotsPtr = 0;
    *sizePtr = 4096-8;
}

int RecordBasedFileManager::getNumberOfSlotsForPagePointer(char* endPtr){
    int* slotPointer = (int*) (endPtr-8);
    return *slotPointer;
}

int RecordBasedFileManager::updateSlotsForPagePointer(char* endPtr,int size_of_data){
    int* slotPointer = (int*) (endPtr - 8);
    *slotPointer += 1;
    int* freeSpacePtr = (int*) (endPtr - 4);
    *freeSpacePtr -= (size_of_data + 8 );
    return *slotPointer - 1; //Number of slots before adding
}

int RecordBasedFileManager::getOffsetFromSlot(int slotNum, char* endPtr){
    if (slotNum == 0){
        return 0;
    }
    int* slotPtr = (int*)(endPtr - (SLOT_SIZE * slotNum) - 8);
    return *slotPtr;
}

int RecordBasedFileManager::getMaxOffsetFromSlot(int slotNum, char* endPtr){
    if (slotNum == 0){
        return 0;
    }
    char* slotPtr = (char*)(endPtr-8);
    int maxOffset = 0;
    int sizeOfData = 0;
    for(int i =0 ; i < slotNum; i++){
        int* slotOffset = (int*)(slotPtr - ((SLOT_SIZE * i) + 8));
        int* slotSize = (int*)(slotPtr - ((SLOT_SIZE * i) + 4));
        if(*slotOffset >= maxOffset){
            maxOffset = *slotOffset;
            sizeOfData = *slotSize;
        }
    }
    return maxOffset+ sizeOfData;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
    void* returnedData = malloc(PAGE_SIZE);//8
    int rc = RecordBasedFileManager::readRecord(fileHandle, recordDescriptor, rid, returnedData);
    if(rc != 0){
        cout<<"READ FOR RID HAS FAILED " << rid.pageNum << " " << rid.slotNum << endl;
    }
    char* charDataPointer = (char*)returnedData;
    int sizeOfRecordDescriptor = recordDescriptor.size();
    int leftmostFlagByteSize;
    if(sizeOfRecordDescriptor%8==0)
    {
        leftmostFlagByteSize = sizeOfRecordDescriptor/8;
    }
    else
        leftmostFlagByteSize = 1 + sizeOfRecordDescriptor/8;
    charDataPointer+=leftmostFlagByteSize;
    int nullOffsetReturnAttr = leftmostFlagByteSize;


    int sizeOfAttribute = 0;
    bool attrFound = false;
    int nullFlagIdx = 0;
    int bitChecker = 7;
    bool isNull = false;
    for(vector<Attribute>::const_iterator recordDescriptor_itr = recordDescriptor.begin(); recordDescriptor_itr != recordDescriptor.end(); recordDescriptor_itr++)
    {
//        isNull = *((char*)data + nullFlagIdx) & 1 << bitChecker;
//        if(bitChecker == 0){
//            nullFlagIdx++;
//            bitChecker = 7;
//        }
//        else{
//            bitChecker--;
//        }


        Attribute attr = *recordDescriptor_itr;
        if(attr.name == attributeName){
            attrFound = true;
        }
        if(attr.type == TypeInt)
        {
            if(attrFound){
                sizeOfAttribute = 4;
                break;
            }
            cout<<"attribute name: "<<attr.name<<", attribute type: int, attribute length: "<<attr.length<<", attribute data: "<<*(int*)charDataPointer<<"\n";
            charDataPointer += 4;
        }

        if(attr.type == TypeReal)
        {
            if(attrFound){
                sizeOfAttribute = 4;
                break;
            }
            cout<<"attribute name: "<<attr.name<<", attribute type: real, attribute length: "<<attr.length<<", attribute data: "<<*(float*)charDataPointer<<"\n";
            charDataPointer += 4;
        }

        if(attr.type == TypeVarChar)
        {
            cout<<"attribute name: "<<attr.name<<", attribute type: varchar, attribute length: "<<attr.length<<", attribute data: ";
            int* size = (int*)charDataPointer; //type cast
            charDataPointer += 4;
            if(attrFound){
                sizeOfAttribute = *size;
                break;
            }
            for(int size_itr = 0; size_itr < *size; size_itr++)
            {
                char charDataToPrint = *charDataPointer;
                cout<<charDataToPrint;
                charDataPointer++;
            }
            cout<<"\n";
        }
    }
    if(attrFound){
//        if(isNull){
//            *((char*)data) = *((char*)data) | 1 << 7;
//            memset((char*)data+nullOffsetReturnAttr,0,sizeOfAttribute);
//            return 0;
//        }
        memcpy((char*)data+nullOffsetReturnAttr,charDataPointer,sizeOfAttribute);
        *((char*)data+sizeOfAttribute) = NULL;
        free(returnedData); //8
        return 0;
    }
    free(returnedData);//8
    return -1;

}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value,const vector<string> &attributeNames,RBFM_ScanIterator &rbfm_ScanIterator){

    int numberOfPages = fileHandle.getNumberOfPages();
    for(int iterPages = 0; iterPages < numberOfPages; iterPages++){
        void* dataRead = malloc(PAGE_SIZE);//9
        int ReadingResult = fileHandle.readPage(iterPages,dataRead);
        if(ferror (fileHandle.filePointer)){
            cout << "ERROR READING PAGE"<<"\n";
        }
        if(ReadingResult != 0){
            return - 1;
        }
        char* endOfPageData = (char*)(dataRead) + PAGE_SIZE;
        int numberOfSlots = RecordBasedFileManager::getNumberOfSlotsForPagePointer(endOfPageData);
        free(dataRead);
        for( int slotIter = 1;slotIter <= numberOfSlots;slotIter++){
            RID rid;
            rid.pageNum = iterPages;
            rid.slotNum = slotIter;
            void* data = malloc(PAGE_SIZE);//10
            int ra = RecordBasedFileManager::readRecord(fileHandle, recordDescriptor, rid, data);
            if(ra == 0){
                if(readRecordValid(data,compOp,value,recordDescriptor,conditionAttribute)){
                    rbfm_ScanIterator.scanRID.push_back(rid);
                    rbfm_ScanIterator.attributeNames = attributeNames;
                    rbfm_ScanIterator.filehandler = fileHandle;
                    rbfm_ScanIterator.sizeOfRIDRecord.push_back(RecordBasedFileManager::sizeOfRecord(fileHandle,recordDescriptor,rid,data));
                    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
                }
            }
            free(data);//10
        }
    }
    return 0;
    
}

bool RecordBasedFileManager::readRecordValid(void* data,const CompOp compOp,const void* value, const vector<Attribute> &recordDescriptor, const string &conditionAttribute){
    AttrType typeAttr;
    bool isNull;
    if(conditionAttribute.size() == 0){
        return true;
    }
    if(compOp!=NO_OP && value==NULL){
        return  false;
    }
    int offsetOfAttribute = RecordBasedFileManager::findSizeUptoAnAttribute((char*)data,recordDescriptor,conditionAttribute, &typeAttr,&isNull);
    if(isNull){
        return false;
    }
    if(typeAttr == TypeInt)
    {
        char* dataOfAttr = (char*)data + offsetOfAttribute;
        if(compOp == EQ_OP){
            if(*(int*)value == *(int*)dataOfAttr){
                return true;
            }
            return false;
        }
        else if(compOp == LT_OP){
            if(*(int*)value > *(int*)dataOfAttr){
                return true;
            }
            return false;
        }
        else if(compOp == LE_OP){
            if(*(int*)value >= *(int*)dataOfAttr){
                return true;
            }
            return false;
        }
        else if(compOp == GT_OP){
            if( *(int*)value < *(int*)dataOfAttr){
                return true;
            }
            return false;
        }
        else if(compOp == GE_OP){
            if(*(int*)value <= *(int*)dataOfAttr){
                return true;
            }
            return false;
        }
        else if(compOp == NE_OP){
            if(*(int*)value != *(int*)dataOfAttr){
                return true;
            }
            return false;
        }
        else{
            return true;                        //     NO_OP OPERAND
        }

    }
    else if(typeAttr == TypeReal)
    {
        char* dataOfAttr = (char*)data + offsetOfAttribute;
        if(compOp == EQ_OP){
            if(*(float*)value == *(float*)dataOfAttr){
                return true;
            }
            return false;
        }
        else if(compOp == LT_OP){
            if(*(float*)value > *(float*)dataOfAttr){
                return true;
            }
            return false;
        }
        else if(compOp == LE_OP){
            if(*(float*)value >= *(float*)dataOfAttr){
                return true;
            }
            return false;
        }
        else if(compOp == GT_OP){
            if( *(float*)value < *(float*)dataOfAttr){
                return true;
            }
            return false;
        }
        else if(compOp == GE_OP){
            if(*(float*)value <= *(float*)dataOfAttr){
                return true;
            }
            return false;
        }
        else if(compOp == NE_OP){
            if(*(float*)value != *(float*)dataOfAttr){
                return true;
            }
            return false;
        }
        else{
            return true;                        //     NO_OP OPERAND
        }
    }
    else if(typeAttr == TypeVarChar)
    {
        char* sizeOfAttr = (char*)data + offsetOfAttribute;
        char* sizeOfValue = (char*)value;
        int* sizeValue = (int*)sizeOfValue;
        int* size = (int*)sizeOfAttr; //type cast
        char* dataOfValue = (char*)value + 4;
        char* dataOfAttr = (char*)data + offsetOfAttribute + 4;
        if(*size != * sizeValue){
            return  false;
        }
        int count = 0;
        for(int size_itr = 0; size_itr < *size; size_itr++)
        {
            char charAttrData = *(dataOfAttr + size_itr);
            char charValueData = *(dataOfValue + size_itr);
            if(charAttrData == charValueData ) {
                count++;
            }
        }
        if(count == *size){
            return true;
        }
    }
    return false;
}

RC RBFM_ScanIterator::close()
{
    return 0;
}

