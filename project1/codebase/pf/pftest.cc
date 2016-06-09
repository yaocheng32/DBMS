#include <iostream>
#include <string>
#include <cassert>
#include <stdio.h>
#include <cstring>
#include <stdlib.h>
#include <sys/stat.h>

#include "pf.h"

using namespace std;

const int success = 0;


// Check if a file exists
bool FileExists(string fileName)
{
    struct stat stFileInfo;

    if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
    else return false;
}

int PFTest_1(PF_Manager *pf)
{
    // Functions Tested:
    // 1. CreateFile
    cout << "****In PF Test Case 1****" << endl;

    RC rc;
    string fileName = "test";

    // Create a file named "test"
    rc = pf->CreateFile(fileName.c_str());
    assert(rc == success);

    if(FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been created." << endl << endl;
        //return 0;
    }
    else
    {
        cout << "Failed to create file!" << endl;
        //return -1;
    }

    // Create "test" again, should fail
    rc = pf->CreateFile(fileName.c_str());
    //printf("%d\n", rc);
    assert(rc != success);

    //remove("test");

    return 0;
}

int PFTest_2(PF_Manager *pf)
{
    // Functions Tested:
    // 1. OpenFile
    // 2. AppendPage
    // 3. GetNumberOfPages
    // 4. WritePage
    // 5. ReadPage
    // 6. CloseFile
    // 7. DestroyFile
    cout << "****In PF Test Case 2****" << endl;

    RC rc;
    string fileName = "test";

    // Open the file "test"
    PF_FileHandle fileHandle;
    rc = pf->OpenFile(fileName.c_str(), fileHandle);
    assert(rc == success);
    
    // Append the first page
    // Write ASCII characters from 32 to 125 (inclusive)
    void *data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 94 + 32;
    }
    rc = fileHandle.AppendPage(data);
    assert(rc == success);
   
    // Get the number of pages
    unsigned count = fileHandle.GetNumberOfPages();
    assert(count == (unsigned)1);

    // Update the first page
    // Write ASCII characters from 32 to 41 (inclusive)
    data = malloc(PF_PAGE_SIZE);
    for(unsigned i = 0; i < PF_PAGE_SIZE; i++)
    {
        *((char *)data+i) = i % 10 + 32;
    }
    rc = fileHandle.WritePage(0, data);
    assert(rc == success);

    // Read the page
    void *buffer = malloc(PF_PAGE_SIZE);
    rc = fileHandle.ReadPage(0, buffer);
    assert(rc == success);

    // Check the integrity
    rc = memcmp(data, buffer, PF_PAGE_SIZE);
    assert(rc == success);
 
    // Close the file "test"
    rc = pf->CloseFile(fileHandle);
    assert(rc == success);

    free(data);
    free(buffer);

    // DestroyFile
    rc = pf->DestroyFile(fileName.c_str());
    assert(rc == success);
    
    if(!FileExists(fileName.c_str()))
    {
        cout << "File " << fileName << " has been destroyed." << endl;
        cout << "Test Case 2 Passed!" << endl << endl;
        return 0;
    }
    else
    {
        cout << "Failed to destroy file!" << endl;
        return -1;
    }
}


void *createChuck(char c) {
	void *data = malloc(PF_PAGE_SIZE);
	for (int i = 0; i < PF_PAGE_SIZE; i++) {
		*((char *)data + i) = c;
	}
	return data;
}

void printChuck(void *data) {
	for (int i = 0; i < PF_PAGE_SIZE; i++) {
		printf("%c", *((char *)data + i));
	}
}


// tests createfile, destroyfile
int myTest1(PF_Manager *pf) {
	remove("test");
	printf("My test 1... ");
	RC rc;
	string fileName = "test";
	rc = pf->DestroyFile(fileName.c_str());
	assert(rc != success);
	rc = pf->CreateFile(fileName.c_str());
	assert(rc == success);
	rc = pf->CreateFile(fileName.c_str());
	assert(rc != success);	
	rc = pf->DestroyFile(fileName.c_str());
	assert(rc == success);
	printf("passed!\n");
	remove("test");
	return 0;
}

// tests openfile, appendpage
int myTest2(PF_Manager *pf) {
	remove("test");
	printf("My test 2... ");
	RC rc;
	string fileName = "test";
	PF_FileHandle fileHandle;
	rc = pf->OpenFile(fileName.c_str(), fileHandle);
	assert(rc != success);
	rc = pf->CreateFile(fileName.c_str());
	assert(rc == success);
	rc = pf->OpenFile(fileName.c_str(), fileHandle);
	assert(rc == success);

	// append 1000 times
	void *data = malloc(PF_PAGE_SIZE);
	for (int i = 0; i < 1000; i++) {
		//printf("used = %d\n", fileHandle.current_buffer_used);
		//printf("size = %d\n", fileHandle.current_buffer_size);
		rc = fileHandle.AppendPage(data);
		assert(rc == success);
	}

	pf->CloseFile(fileHandle);
	assert(fileHandle.GetNumberOfPages() == 0);

	printf("passed!\n");
	//remove("test");
	return 0;
}

int myTest3(PF_Manager *pf) {
	printf("My test 3... ");
	remove("test3");

	RC rc;
	string fileName = "test3";
	PF_FileHandle fileHandle;
	rc = pf->CreateFile(fileName.c_str());
	assert(rc == success);	
	rc = pf->OpenFile(fileName.c_str(), fileHandle);
	assert(rc == success);

	// write before append
	rc = fileHandle.WritePage(0, createChuck((char)97));
	assert(rc != success);

	rc = fileHandle.AppendPage(createChuck((char)97));
	assert(rc == success);
	assert(fileHandle.GetNumberOfPages() == (unsigned)1);
	rc = pf->CloseFile(fileHandle);
	assert(rc == success);

	// int tmp;
	// scanf("Stop %d", &tmp);
	//return 0;
	rc = pf->OpenFile(fileName.c_str(), fileHandle);
	assert(rc == success);
	//printf("~~%d\n", fileHandle.GetNumberOfPages());
	assert(fileHandle.GetNumberOfPages() == (unsigned)1);


	rc = fileHandle.AppendPage(createChuck((char)98));
	assert(rc == success);
	rc = fileHandle.AppendPage(createChuck((char)99));
	assert(rc == success);

	rc = fileHandle.WritePage(1, createChuck((char)100));
	assert(rc == success);


	rc = fileHandle.WritePage(3, createChuck((char)100));
	assert(rc != success);

	void *buf = malloc(PF_PAGE_SIZE);
	rc = fileHandle.ReadPage(3, buf);
	assert(rc != success);
	rc = fileHandle.ReadPage(2, buf);
	assert(rc == success);
	rc = memcmp(buf, createChuck((char)99), PF_PAGE_SIZE);
	assert(rc == success);

	rc = fileHandle.WritePage(0, createChuck((char)101));
	assert(rc == success);


	rc = pf->CloseFile(fileHandle);
	assert(rc == success);


	free(buf);
	printf("passed!\n");
	return 0;
}
// tests test3 eeedddccc
int myTest4(PF_Manager *pf)
{
	printf("My test 4... ");
	
	RC rc;
	string fileName = "test3";
	PF_FileHandle fileHandle;
	rc = pf->CreateFile(fileName.c_str());
	assert(rc != success);	
	rc = pf->OpenFile(fileName.c_str(), fileHandle);
	assert(rc == success);
	
	assert(fileHandle.GetNumberOfPages() == (unsigned)3);

	for (int i = 0; i < 100; i++) {
		for (int j = 0; j < 26; j++) {
			rc = fileHandle.AppendPage(createChuck((char)(j+97)));
			assert(rc == success);
		}
	}


	rc = pf->CloseFile(fileHandle);
	assert(rc == success);


	printf("passed!\n");
	return 0;
}


// tests fileHandle
int myTest5(PF_Manager *pf)
{
	printf("My test 5... ");

	RC rc;
	string fileName1 = "test3";
	string fileName2 = "test5";
	PF_FileHandle fileHandle;

	rc = pf->CloseFile(fileHandle);
	assert(rc != success);

	rc = pf->CreateFile(fileName2.c_str());
	assert(rc == success);
	rc = pf->OpenFile(fileName2.c_str(), fileHandle);
	assert(rc == success);

	rc = pf->OpenFile(fileName1.c_str(), fileHandle);
	assert(rc != success);

	rc = pf->CloseFile(fileHandle);
	assert(rc == success);

	rc = pf->OpenFile(fileName1.c_str(), fileHandle);
	assert(rc == success);

	rc = pf->CloseFile(fileHandle);
	assert(rc == success);

	// PF_FileHandle* fileHandleptr = new PF_FileHandle();

	// rc = pf->OpenFile(fileName1.c_str(), *fileHandleptr);
	// assert(rc == success);

	// rc = pf->CloseFile(*fileHandleptr);
	// assert(rc == success);

	remove("test5");
	printf("passed!\n");
	return 0;
}

int myTest6(PF_Manager *pf)
{
	printf("My test 6... ");
	printf("passed!\n");
	return 0;
}


int main()
{
    PF_Manager *pf = PF_Manager::Instance();
    remove("test");
   
    PFTest_1(pf);
    PFTest_2(pf); 
    remove("test");
    myTest1(pf);
    myTest2(pf);
    myTest3(pf);
    myTest4(pf);
    myTest5(pf);
    
    return 0;
}