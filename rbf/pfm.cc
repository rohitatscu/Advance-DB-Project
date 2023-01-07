#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    FILE *file = fopen(fileName.c_str(), "rb");
    if(!file)
    {
        //file does not exist
        file = fopen(fileName.c_str(), "wb+");
        fclose(file);
        return 0;
    }
    return -1;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
    if(remove(fileName.c_str())!=0)
        return -1;
    else
        return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    if(FileExist(fileName) && fileHandle.IsFree()){
        FILE *file = fopen(fileName.c_str(), "rb+");
        FileHandle newFileHandle;
        fileHandle = newFileHandle;
        fileHandle.SetHandler(file);
        fileHandle.fileName = fileName;
        fileHandle.numberOfPages = fileHandle.getNumberOfPages();
        return 0;
    }
    return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if(fileHandle.IsFree()){
        return -1;
    }
    fileHandle.FreeHandler(fileHandle);
    return 0;
}

bool PagedFileManager::FileExist(const string &fileName) {
    FILE *file = fopen(fileName.c_str(), "rb");
    if(file){
        return true;
    }
    return false;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    numberOfPages = 0;
    filePointer = NULL;
}


FileHandle::~FileHandle()
{
    
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if(FileHandle::numberOfPages==0)
        return -1;
    if(pageNum + 1 > FileHandle::numberOfPages)
        return -1;
    if(!FileHandle::IsFree()){
        int StartReadingFrom = fseek(FileHandle::filePointer, pageNum*PAGE_SIZE, SEEK_SET); //offsetting w.r.t page pageNum
        if(StartReadingFrom!=0)
            return -1;
        int ReadingResult = fread(data, sizeof(char), PAGE_SIZE, FileHandle::filePointer); //Read operation
        if(ReadingResult!=PAGE_SIZE) //unsuccessful read -> Exit Failure
            return -1;
        FileHandle::readPageCounter++; //Increase readPageCounter incase of successful read operation
        return 0;
    }    
    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if(FileHandle::numberOfPages==0)
        return -1;
    if(pageNum + 1 > FileHandle::numberOfPages)
        return -1;
    if(!FileHandle::IsFree()){
        int startWritingFrom = fseek(FileHandle::filePointer, pageNum*PAGE_SIZE, SEEK_SET); //offsetting w.r.t page pageNum
        if(startWritingFrom!=0)
            return -1;
        int writingResult = fwrite(data, sizeof(char), PAGE_SIZE, FileHandle::filePointer); //Write operation
        if(writingResult!=PAGE_SIZE) //unsuccessful write -> Exit Failure
            return -1;
        FileHandle::writePageCounter++; //Increase writePageCounter incase of successful read operation
        return 0;
    }    
    return -1;
}


RC FileHandle::appendPage(const void *data)
{

    if(!FileHandle::IsFree()){
        FileHandle::numberOfPages = FileHandle::getNumberOfPages();
        fseek(FileHandle::filePointer, 0, SEEK_END);
        int writingResult = fwrite(data,sizeof(char),PAGE_SIZE,FileHandle::filePointer);
        if(writingResult!=PAGE_SIZE) //unsuccessful write -> Exit Failure
            return -1;
        FileHandle::appendPageCounter++;
        FileHandle::numberOfPages = FileHandle::getNumberOfPages();
        return 0;
    }
    return -1;
}


unsigned FileHandle::getNumberOfPages()
{
//    FILE *file = fopen(FileHandle::fileName.c_str(), "rb+");
    FILE *file = FileHandle::filePointer;
    fseek(file, 0, SEEK_END);
    unsigned sizeInBytes = ftell(file);
    unsigned numberOfPages = sizeInBytes/PAGE_SIZE;
//    printf("%ld",numberOfPages);
    return numberOfPages;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    if(!FileHandle::IsFree()){
        readPageCount = FileHandle::readPageCounter;
        writePageCount = FileHandle::writePageCounter;
        appendPageCount = FileHandle::appendPageCounter;
        return 0;
    }
    return -1;
}

void FileHandle::SetHandler(FILE *pFile) {
    FileHandle::filePointer = pFile;
}

bool FileHandle::IsFree() {
    if(FileHandle::filePointer){
        return false;
    }
    return true;
}

void FileHandle::FreeHandler(FileHandle &fileHandle) {
    if(!fileHandle.IsFree()){
        if(ferror (fileHandle.filePointer)){
            cout << "ERROR CLOSING 1 PAGE"<<"\n";
        }
        fflush(fileHandle.filePointer);
        if(ferror (fileHandle.filePointer)){
            cout << "ERROR CLOSIGN 2 PAGE"<<"\n";
        }
        fclose(fileHandle.filePointer);
        if(ferror (fileHandle.filePointer)){
            cout << "ERROR CLosing 3 PAGE"<<"\n";
        }
        fileHandle.filePointer = NULL;
    }
}

unsigned FileHandle::computeNumberOfPages(const string &fileName) {
    FILE *file = fopen(fileName.c_str(), "rb+");
    fseek(file, 0, SEEK_END);
    unsigned int sizeInBytes = ftell(file);
    unsigned int numberOfPages = sizeInBytes/PAGE_SIZE;
//    printf("%ld",numberOfPages);
    return numberOfPages;
}
