
#include <fstream>
#include <iostream>
#include <cassert>

#include "rm.h"

// For mkdir
#include <sys/stat.h>
#include <sys/types.h>

using namespace std;

RM *rm;// = RM::Instance();
const int success = 0;

void prepareTuple(const int name_length, const string name, const int age, const float height, const int salary, void *buffer, int *tuple_size)
{
	int offset = 0;

	memcpy((char *)buffer + offset, &name_length, sizeof(int));
	offset += sizeof(int);    
	memcpy((char *)buffer + offset, name.c_str(), name_length);
	offset += name_length;

	memcpy((char *)buffer + offset, &age, sizeof(int));
	offset += sizeof(int);

	memcpy((char *)buffer + offset, &height, sizeof(float));
	offset += sizeof(float);

	memcpy((char *)buffer + offset, &salary, sizeof(int));
	offset += sizeof(int);

	*tuple_size = offset;
}

void prepareTuple2(const float num, const int len, const string name, const int age, void *buffer, int *tuple_size)
{
	// int offset = 0;

	// memcpy((char *)buffer + offset, &name_length, sizeof(int));
	// offset += sizeof(int);    
	// memcpy((char *)buffer + offset, name.c_str(), name_length);
	// offset += name_length;

	// memcpy((char *)buffer + offset, &age, sizeof(int));
	// offset += sizeof(int);

	// memcpy((char *)buffer + offset, &height, sizeof(float));
	// offset += sizeof(float);

	// memcpy((char *)buffer + offset, &salary, sizeof(int));
	// offset += sizeof(int);

	// *tuple_size = offset;
}


// Function to parse the data in buffer and print each field
void printTuple(const void *buffer, const int tuple_size)
{
	int offset = 0;
	cout << "****Printing Buffer: Start****" << endl;

	int name_length = 0;     
	memcpy(&name_length, (char *)buffer+offset, sizeof(int));
	offset += sizeof(int);
	cout << "name_length: " << name_length << endl;

	char *name = (char *)malloc(100);
	memcpy(name, (char *)buffer+offset, name_length);
	name[name_length] = '\0';
	offset += name_length;
	cout << "name: " << name << endl;

	int age = 0; 
	memcpy(&age, (char *)buffer+offset, sizeof(int));
	offset += sizeof(int);
	cout << "age: " << age << endl;

	float height = 0.0; 
	memcpy(&height, (char *)buffer+offset, sizeof(float));
	offset += sizeof(float);
	cout << "height: " << height << endl;

	int salary = 0; 
	memcpy(&salary, (char *)buffer+offset, sizeof(int));
	offset += sizeof(int);
	cout << "salary: " << salary << endl;

	cout << "****Printing Buffer: End****" << endl << endl;    
}

void rmTest()
{
  // RM *rm = RM::Instance();

  // write your own testing cases here
}

void createTheTable1() {

	string tablename = "Mytable";

	cout << "****Create Table " << tablename << " ****" << endl;

    // 1. Create Table ** -- made separate now.
	vector<Attribute> attrs;

	Attribute attr;
	attr.name = "EmpName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)30;
	attrs.push_back(attr);

	attr.name = "Age";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	attrs.push_back(attr);

	attr.name = "Height";
	attr.type = TypeReal;
	attr.length = (AttrLength)4;
	attrs.push_back(attr);

	attr.name = "Salary";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	attrs.push_back(attr);

	RC rc = rm->createTable(tablename, attrs);
	assert(rc == success);
	cout << "****Table Created: " << tablename << " ****" << endl << endl;

}


void createTheTable2() {

	string tablename = "PleaseScanMe";

	cout << "****Create Table " << tablename << " ****" << endl;

    // 1. Create Table ** -- made separate now.
	vector<Attribute> attrs;

	Attribute attr;

	attr.name = "ThisNumber";
	attr.type = TypeReal;
	attr.length = (AttrLength)4;
	attrs.push_back(attr);


	attr.name = "ThisName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)30;
	attrs.push_back(attr);

	attr.name = "ThisStr";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)100;
	attrs.push_back(attr);

	attr.name = "ThisAge";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	attrs.push_back(attr);

	RC rc = rm->createTable(tablename, attrs);
	assert(rc == success);
	cout << "****Table Created: " << tablename << " ****" << endl << endl;

}


void insert1() {
	string tablename = "Mytable";
	cout << "****Insert record1 to " << tablename << " start****" << endl;
	int name_length = 6;
	string name = "Peters";
	int age = 24;
	float height = 170.1;
	int salary = 5000;
	int tuple_size = 0;
	void *tuple = malloc(100);
	RID rid;
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	//printTuple(tuple, tuple_size);
	RC rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);
	printf("pageNum = %d, slotNum = %d\n", rid.pageNum, rid.slotNum);
	cout << "****Insert record1 to " << tablename << " end****" << endl;
}

void insert2() {
	string tablename = "Mytable";
	//cout << "****Insert record2 to " << tablename << " start****" << endl;
	int name_length = 11;
	string name = "Mr.Chalmers";
	int age = 29;
	float height = 188.2;
	int salary = 15000;
	int tuple_size = 0;
	void *tuple = malloc(100);
	RID rid;
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	//printTuple(tuple, tuple_size);
	RC rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);
	printf("pageNum = %d, slotNum = %d\n", rid.pageNum, rid.slotNum);	
	if(rid.slotNum == -1) exit(-1);
	//cout << "****Insert record2 to " << tablename << " end****" << endl;
}

void closeTheTable1() {
	string tablename = "Mytable";
	RC rc = rm->deleteTable(tablename);
	assert(rc == success);
	cout << "****Table Deleted: " << tablename << " ****" << endl << endl;
}

void closeTheTable2() {
	string tablename = "PleaseScanMe";
	RC rc = rm->deleteTable(tablename);
	assert(rc == success);
	cout << "****Table Deleted: " << tablename << " ****" << endl << endl;
}

void Test1() {
	cout << "****Test1 start****" << endl;

	rm = RM::Instance();

	
	RC rc;
	string tablename = "Mytable";

	int name_length = 6;
	string name = "Peters";
	int age = 24;
	float height = 170.1;
	int salary = 5000;
	int tuple_size = 0;
	void *tuple = malloc(100);
	RID rid;
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);

	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);

	void *tupleread = malloc(100);
	rc = rm->readTuple(tablename, rid, tupleread);
	assert(rc == success);
	rc = memcmp(tuple, tupleread, tuple_size);
	assert(rc == success);	

	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);
	rc = rm->readTuple(tablename, rid, tupleread);
	assert(rc == success);
	rc = memcmp(tuple, tupleread, tuple_size);
	assert(rc == success);

	// for (int i = 0; i < tuple_size; i++) {
	// 	printf("%c %c__\n", ((char*)tuple)[i], ((char*)tupleread)[i]);
	// }
	printTuple(tupleread, tuple_size);

	free(tuple);
	free(tupleread);

	cout << "****Test1 end****" << endl;
}

// tests simple scan
void Test2() {
	cout << "****Test2 start****" << endl;
	//printf("here\n");
	rm = RM::Instance();
	
	RC rc;
	string tablename = "Mytable";

	int name_length = 6;
	string name = "Peters";
	int age = 24;
	float height = 170.1;
	int salary = 5000;
	int tuple_size = 0;
	void *tuple = malloc(100);
	RID rid;
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);

	name_length = 10;
	name = "Helloworld";
	age = 30;
	height = 120.1;
	salary = 500000;
	tuple_size = 0;
	tuple = malloc(100);
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);

	name_length = 12;
	name = "James Harden";
	age = 22;
	height = 200.1;
	salary = 2300000;
	tuple_size = 0;
	tuple = malloc(100);
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);


	//printf("here\n");
	RM_ScanIterator rmsi;
  string attr = "Height";
  vector<string> attributes;
  attributes.push_back(attr);
  attributes.push_back("Age");
  attributes.push_back("Salary");
  attributes.push_back("EmpName");  
  //int value = 22;
	//rc = rm->scan(tablename, "", NO_OP, NULL, attributes, rmsi);


	int tmp = 10;	
	void *forscanvalue = malloc(4+tmp);
	memcpy(forscanvalue, &tmp, 4);
	memcpy((char*)forscanvalue+4, "Helloworld", tmp);
	rc = rm->scan(tablename, "EmpName", EQ_OP, forscanvalue, attributes, rmsi);	
	free(forscanvalue);
	assert(rc == success);

	void *data = malloc(200);

	while (rmsi.getNextTuple(rid, data) != -1) {
		//float h = 0;
		//memcpy((void*)&h, data, 4);
		//printf("h = %f\n", h);
		//printTuple(data, 26);
	}
	free(data);
	free(tuple);

	cout << "****Test2 end****" << endl;
}

// tests read
void Test3() {
	cout << "****Test3 start****" << endl;

	rm = RM::Instance();
	
	RC rc;
	string tablename = "Mytable";

	int name_length = 6;
	string name = "Peters";
	int age = 24;
	float height = 170.1;
	int salary = 5000;
	int tuple_size = 0;
	void *tuple = malloc(100);
	RID rid;
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);

	name_length = 10;
	name = "Helloworld";
	age = 30;
	height = 120.1;
	salary = 500000;
	tuple_size = 0;
	tuple = malloc(100);
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);


	RID rid2 = rid;
	void *tuple2 = malloc(100);
	int tuple_size2 = tuple_size;
	string name2 = name;
	int salary2 = salary;
	memcpy(tuple2, tuple, tuple_size2);

	name_length = 12;
	name = "James Harden";
	age = 22;
	height = 200.1;
	salary = 2300000;
	tuple_size = 0;
	tuple = malloc(100);
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);

	void *tupleread = malloc(100);
	rc = rm->readTuple(tablename, rid2, tupleread);

	assert(rc == success);
	rc = memcmp(tuple2, tupleread, tuple_size2);
	assert(rc == success);

		
	void *attrread = malloc(100);
	rc = rm->readAttribute(tablename, rid2, "EmpName", attrread);
	assert(rc == success);
	//printf("here\n");
	rc = memcmp(name2.c_str(), (char*)attrread+4, 10);
	assert(rc == success);

	rc = rm->readAttribute(tablename, rid2, "Salary", attrread);
	assert(rc == success);
	rc = memcmp(&salary2, attrread, 4);
	assert(rc == success);

	rc = rm->readAttribute(tablename, rid2, "Salary2", attrread);
	assert(rc != success);

	rid2.pageNum = 100;
	rc = rm->readTuple(tablename, rid2, tupleread);
	assert(rc != success);

	free(tuple);
	free(tupleread);
	cout << "****Test3 end****" << endl;
}


void insertMany() {
	for (int i = 0; i < 1000; i++) {
		insert2();
	}
}


// test scan
// void Test4() {
// 	cout << "****Test4 start****" << endl;

// 	rm = RM::Instance();
	
// 	RC rc;
// 	string tablename = "PleaseScanMe";

// 	float thisnumber = 3;
// 	int len = 9;
// 	string name = "Yamahaoju";
// 	int len = 10;
// 	string name = "uuukkkolea";
// 	int age = 24;

// 	int tuple_size = 0;
// 	void *tuple = malloc(100);
// 	RID rid;
// 	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
// 	rc = rm->insertTuple(tablename, tuple, rid);
// 	assert(rc == success);

// 	name_length = 10;
// 	name = "Helloworld";
// 	age = 30;
// 	height = 120.1;
// 	salary = 500000;
// 	tuple_size = 0;
// 	tuple = malloc(100);
// 	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
// 	rc = rm->insertTuple(tablename, tuple, rid);
// 	assert(rc == success);


// 	RID rid2 = rid;
// 	void *tuple2 = malloc(100);
// 	int tuple_size2 = tuple_size;
// 	string name2 = name;
// 	int salary2 = salary;
// 	memcpy(tuple2, tuple, tuple_size2);

// 	name_length = 12;
// 	name = "James Harden";
// 	age = 22;
// 	height = 200.1;
// 	salary = 2300000;
// 	tuple_size = 0;
// 	tuple = malloc(100);
// 	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
// 	rc = rm->insertTuple(tablename, tuple, rid);
// 	assert(rc == success);

// 	void *tupleread = malloc(100);
// 	rc = rm->readTuple(tablename, rid2, tupleread);
// 	assert(rc == success);
// 	rc = memcmp(tuple2, tupleread, tuple_size2);
// 	assert(rc == success);


// 	void *attrread = malloc(100);
// 	rc = rm->readAttribute(tablename, rid2, "EmpName", attrread);
// 	assert(rc == success);
// 	rc = memcmp(name2.c_str(), (char*)attrread+4, 10);
// 	assert(rc == success);
// 	rc = rm->readAttribute(tablename, rid2, "Salary", attrread);
// 	assert(rc == success);
// 	rc = memcmp(&salary2, attrread, 4);
// 	assert(rc == success);

// 	rc = rm->readAttribute(tablename, rid2, "Salary2", attrread);
// 	assert(rc != success);

// 	rid2.pageNum = 100;
// 	rc = rm->readTuple(tablename, rid2, tupleread);
// 	assert(rc != success);


// 	cout << "****Test4 end****" << endl;
// }


// test reorg
void Test4() {
	cout << "****Test4 start****" << endl;

	rm = RM::Instance();
	
	RC rc;
	string tablename = "Mytable";
	int name_length = 12;
	string name = "James Harden";
	int age = 22;
	float height = 200.1;
	int salary = 2300000;
	int tuple_size = 0;
	void* tuple = malloc(100);
	RID rid;
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);
	RID rid2 = rid;
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);
	rc = rm->deleteTuple(tablename, rid2);
	assert(rc == success);
	//void *tupleread = malloc(100);
	printf("Before reorg\n");
	int tmp;
	//cin >> tmp;
	rc = rm->reorganizePage(tablename, 0);
	assert(rc == success);

	cout << "****Test4 end****" << endl;
}


// test delete
void Test5() {
	cout << "****Test5 start****" << endl;

	rm = RM::Instance();
	
	RC rc;
	string tablename = "Mytable";

	int name_length = 6;
	string name = "Peters";
	int age = 24;
	float height = 170.1;
	int salary = 5000;
	int tuple_size = 0;
	void *tuple = malloc(100);
	RID rid;
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	void *tupleread = malloc(100);
	//printf("1\n");
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);
	rc = rm->readTuple(tablename, rid, tupleread);
	assert(rc == success);


	rc = rm->deleteTuple(tablename, rid);
	assert(rc == success);
	//printf("2\n");

	//printf("%d %d\n", rid.pageNum, rid.slotNum);
	rc = rm->readTuple(tablename, rid, tupleread);
	assert(rc != success);


	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);
	// printf("here\n");	
	rc = rm->readTuple(tablename, rid, tupleread);
	assert(rc == success);

	rc = rm->deleteTuples(tablename);
	assert(rc == success);

	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);
	rc = rm->readTuple(tablename, rid, tupleread);
	assert(rc == success);

	cout << "****Test5 end****" << endl;	
}


// test no table name
void Test6() {
	cout << "****Test6 start****" << endl;

	rm = RM::Instance();
	
	RC rc;
	string tablename = "Thereisnothistable";
	int name_length = 6;
	string name = "Peters";
	int age = 24;
	float height = 170.1;
	int salary = 5000;
	int tuple_size = 0;
	void *tuple = malloc(100);
	RID rid;
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	void *tupleread = malloc(100);

	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc != success);
	rc = rm->readTuple(tablename, rid, tupleread);
	assert(rc != success);
	rc = rm->deleteTuple(tablename, rid);
	assert(rc != success);
	rc = rm->deleteTuples(tablename);
	assert(rc != success);	
	rc = rm->updateTuple(tablename, tuple, rid);
	assert(rc != success);

	cout << "****Test6 end****" << endl;	
}


// test scan
void Test7() {
	cout << "****Test7 start****" << endl;

	rm = RM::Instance();
	
	RC rc;
	string tablename = "Mytable";
	int name_length = 6;
	string name = "Peters";
	int age = 24;
	float height = 170.1;
	int salary = 5000;
	int tuple_size = 0;
	void *tuple = malloc(100);
	RID rid;
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	void *tupleread = malloc(100);

	RM_ScanIterator rmsi;
	vector<string> attributes;
  attributes.push_back("Age");	
	int ageval = 30;
	rc = rm->scan(tablename, "Age", LE_OP, &ageval, attributes, rmsi);
	while (rmsi.getNextTuple(rid, tupleread) != -1) {
		printf("%d\n", *(int*)tupleread);
	}



	cout << "****Test7 end****" << endl;	
}

void TestScan() {
	cout << "****TestScan start****" << endl;

	RC rc;
	string tablename = "Mytable";

	rm = RM::Instance();
	int name_length = 6;
	string name = "Peters";
	int age = 24;
	float height = 170.1;
	int salary = 5000;
	int tuple_size = 0;
	void *tuple = malloc(100);
	RID rid;
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);

	name_length = 10;
	name = "Helloworld";
	age = 30;
	height = 120.1;
	salary = 500000;
	tuple_size = 0;
	tuple = malloc(100);
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);


	RID rid2 = rid;
	void *tuple2 = malloc(100);
	int tuple_size2 = tuple_size;
	string name2 = name;
	int salary2 = salary;
	memcpy(tuple2, tuple, tuple_size2);

	name_length = 12;
	name = "James Harden";
	age = 22;
	height = 200.1;
	salary = 2300000;
	tuple_size = 0;
	tuple = malloc(100);
	prepareTuple(name_length, name, age, height, salary, tuple, &tuple_size);
	rc = rm->insertTuple(tablename, tuple, rid);
	assert(rc == success);

	rc = rm->deleteTuple(tablename, rid2);
	assert(rc == success);

	rc = rm->reorganizePage(tablename, 0);
	assert(rc == success);

	void *tupleread = malloc(100);
	RM_ScanIterator rmsi;
	vector<string> attributes;
  attributes.push_back("Age");	
  attributes.push_back("Height");
  attributes.push_back("EmpName");
  attributes.push_back("Salary");
	int ageval = 2;
	rc = rm->scan(tablename, "Age", GT_OP, &ageval, attributes, rmsi);
	while (rmsi.getNextTuple(rid, tupleread) != -1) {
		printTuple(tupleread, 100);
	}


	cout << "****TestScan end****" << endl;	
}

int main() 
{
	int tmp;
	cout << "test..." << endl;
	system("rm -r Tables");
	string CATALOG_NAME = "Catalog";
	string catarec = CATALOG_NAME+".rec", catamap = CATALOG_NAME+".map";
	remove(catarec.c_str());
	remove(catamap.c_str());
	rm = RM::Instance();

	createTheTable1();
	//insert2();
	// createTheTable2();
	Test1();
	Test2();
	Test3();
	Test4();
	Test5();
	Test6();

	//insertMany();
	//TestScan();
	
	//closeTheTable2();
	
	//closeTheTable1();
  //rmTest();
  // other tests go here

	cout << "OK" << endl;
}
