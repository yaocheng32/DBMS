#include "qe.h"
#include <iostream>
#include <cassert>
#include <cmath>

// For mkdir
#include <sys/stat.h>
#include <sys/types.h>

// For timestamp
#include <ctime>

#include <sstream>

// for testing start

// void prepareTuple(const int name_length, const string name, const int age, const float height, const int job_length, const string job, const int salary, void *buffer, int *tuple_size)
// {
//     int offset = 0;
    
//     memcpy((char *)buffer + offset, &name_length, sizeof(int));
//     offset += sizeof(int);    
//     memcpy((char *)buffer + offset, name.c_str(), name_length);
//     offset += name_length;
    
//     memcpy((char *)buffer + offset, &age, sizeof(int));
//     offset += sizeof(int);
    
//     memcpy((char *)buffer + offset, &height, sizeof(float));
//     offset += sizeof(float);

//     memcpy((char *)buffer + offset, &job_length, sizeof(int));
//     offset += sizeof(int);    
//     memcpy((char *)buffer + offset, job.c_str(), job_length);
//     offset += job_length;
    
//     memcpy((char *)buffer + offset, &salary, sizeof(int));
//     offset += sizeof(int);
    
//     *tuple_size = offset;
// }


// void test_getOneAttrValue() {
// 	void *tuple = malloc(200);
// 	void *value = malloc(100);
// 	int tuple_size;
// 	prepareTuple(5, "Alice", 32, 180.3, 6, "Doctor" ,3000, tuple, &tuple_size);
// 	vector<Attribute> attrs;
// 	Attribute attr;
// 	attr.name = "Name";
// 	attr.type = TypeVarChar;
// 	attr.length = 30;
// 	attrs.push_back(attr);
// 	attr.name = "Age";
// 	attr.type = TypeInt;
// 	attr.length = 4;
// 	attrs.push_back(attr);
// 	attr.name = "Height";
// 	attr.type = TypeReal;
// 	attr.length = 4;
// 	attrs.push_back(attr);
// 	attr.name = "Job";
// 	attr.type = TypeVarChar;
// 	attr.length = 30;
// 	attrs.push_back(attr);
// 	attr.name = "Salary";
// 	attr.type = TypeInt;
// 	attr.length = 4;
// 	attrs.push_back(attr);
// 	int val_len;
// 	void *correct_value = malloc(200);
// 	getOneAttrValue(tuple, value, attrs, "Name", &val_len);
// 	assert(val_len == 9);
// 	memcpy(correct_value, tuple, 9);
// 	assert(0 == memcmp(correct_value, value, 9));

// 	free(tuple);
// 	free(value);
// 	free(correct_value);
// }


// for testing end


int calculateTupleLength(const void *tuple, const vector<Attribute> all_attrs) {
	int length = 0;
	for (unsigned i = 0; i < all_attrs.size(); i++) {
		Attribute attr = all_attrs[i];
		if (attr.type == TypeVarChar) {
			length += (4 + *(int*)((char*)tuple+length));
		} else {
			length += 4;
		}
	}
	return length;
}

// get a attr's value from a given tuple
void getOneAttrValue(const void *tuple, void *value, const vector<Attribute> &all_attrs, const string attrname, int *length) {
	int offset = 0;
	Attribute attr;
	for (unsigned i = 0; i < all_attrs.size(); i++) {
		attr = all_attrs[i];

		// find target
		if (attrname == attr.name) {
			if (attr.type == TypeVarChar) {
				int tmplen;
				memcpy(&tmplen, (char*)tuple+offset, 4);
				memcpy(value, (char*)tuple+offset, 4+tmplen);
				*length = 4+tmplen;
			} else {
				memcpy(value, (char*)tuple+offset, 4);
				*length = 4;
			}
			return;
		}

		// not found, move offset
		if (attr.type == TypeVarChar) {
			int tmplen;
			memcpy(&tmplen, (char*)tuple+offset, 4);
			offset += 4 + tmplen;
		} else {
			offset += 4;
		}
	}
	cout << attrname << " not found in getOneAttrValue!" << endl;
	return;
}

// void mergeTuple(void *output, const string merge_attr_name, const void *left_tuple, const void *right_tuple, const vector<Attribute> left_attrs, const vector<Attribute> right_attrs) {
// 	// check if merge_attr_name is in left & right
// 	bool found = false;
// 	for (unsigned i = 0; i < left_attrs.size(); i++) {
// 		if (merge_attr_name == left_attrs[i].name) {
// 			found = true;
// 			break;
// 		}
// 	}
// 	assert(found);
// 	found = false;
// 	for (unsigned i = 0; i < right_attrs.size(); i++) {
// 		if (merge_attr_name == right_attrs[i].name) {
// 			found = true;
// 			break;
// 		}
// 	}
// 	assert(found);

// 	// start merging
// 	int offset = 0;
// 	void *buf = malloc(PF_PAGE_SIZE);
// 	// copy left
// 	int length = calculateTupleLength(left_tuple, left_attrs);
// 	memcpy(output, left_tuple, length);
// 	offset += length;

// 	// copy right
// 	for (unsigned i = 0; i < right_attrs.size(); i++) {
// 		// skip the merging attribute
// 		if (right_attrs[i].name == merge_attr_name)
// 			continue;

// 		getOneAttrValue(right_tuple, buf, right_attrs, right_attrs[i].name, &length);
// 		memcpy((char*)output+offset, buf, length);
// 	}
// 	free(buf);
// 	return;
// }

void mergeTuple(int & dataLen, void *data, const void *leftBuffer, vector<Attribute> leftAttrs,
		const void *rightBuffer, vector<Attribute> rightAttrs) {
	int leftLen, rightLen;
	leftLen = calculateTupleLength(leftBuffer, leftAttrs);
	rightLen = calculateTupleLength(rightBuffer, rightAttrs);

	memcpy(data, leftBuffer, leftLen);
	memcpy((char*)data + leftLen, rightBuffer, rightLen);
	dataLen = leftLen + rightLen;
}

RC getNextPartitionRecord(void * recordBuffer, unsigned &recordLength, ReadPartition &readPartition) {
	bool found = false;
    while(!found) {
        //Reading tuples within curr page

        while(readPartition.currTupleNum < readPartition.tupleNumOfPage) {
            unsigned len = readPartition.nextRecordLength();
            //void * result = calloc(len, 1);
            memcpy(recordBuffer, (char*)readPartition.pageBuffer + readPartition.currPosition, len);
            readPartition.currPosition += len;
            readPartition.currTupleNum++;
            recordLength = len;
            return 0;
        }
        //Have already read all the tuples in curr page
        if(readPartition.currTupleNum == readPartition.tupleNumOfPage) {
        	readPartition.currPageNum++;
            if(readPartition.currPageNum == readPartition.pageNum) {
                recordLength = 0;
                return -1;
            } else {
            	readPartition.partitionHandle.ReadPage(readPartition.currPageNum, readPartition.pageBuffer);
            	readPartition.currPageNum++;
            	readPartition.currTupleNum = 0;
            }
        }
    }
    return -1;
}

Filter::Filter(Iterator *input, const Condition &condition) {
	if (condition.bRhsIsAttr) {
		cout << "Right hand side is attr in Filter constructor... please check!" << endl;
	}
	Operator = condition.op;
	Attrname = condition.lhsAttr;
	RhsValue = condition.rhsValue;
	Input = input;
	input->getAttributes(all_attrs);
	Tuple_buffer = malloc(PF_PAGE_SIZE);
	Value_buffer = malloc(PF_PAGE_SIZE);

	// Q: check value?
	// Q: check input?
}

Filter::~Filter() {
	free(Tuple_buffer);
	free(Value_buffer);
}

// problem of Tuple_buffer?

RC Filter::getNextTuple(void *data) {
	bool match = false;
	int value_length;
	// iterate to find match
	do {
		if (Input->getNextTuple(Tuple_buffer) == QE_EOF) return QE_EOF;
		getOneAttrValue(Tuple_buffer, Value_buffer, all_attrs, Attrname, &value_length);
		match = matchCondition(Value_buffer);
	} while (!match);

	// found match, copy data
	memcpy(data, Tuple_buffer, calculateTupleLength(Tuple_buffer, all_attrs));
	return 0;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = all_attrs;
}

// salary < 600
bool Filter::matchCondition(const void *value) {
	if (this->Operator == NO_OP) {
		return true;
	}

	// not NO_OP

	// left is value, right is rhsvalue
	bool equal, greater, less;
	AttrType rhs_type = this->RhsValue.type;
	void *rhs_data = this->RhsValue.data;
	if (rhs_type == TypeVarChar) {
		int llen, rlen;
		memcpy(&llen, value, 4);
		memcpy(&rlen, rhs_data, 4);
		string lstr = string((char*)value+4, llen);
		string rstr = string((char*)rhs_data+4, rlen);
		equal = (lstr == rstr);
		greater = (lstr > rstr);
		less = (lstr < rstr);
	} else if (rhs_type == TypeInt) {
		int lval, rval;
		memcpy(&lval, value, 4);
		memcpy(&rval, rhs_data, 4);
		equal = (lval == rval);
		greater = (lval > rval);
		less = (lval < rval);
	} else if (rhs_type == TypeReal) {
		float lval, rval;
		memcpy(&lval, value, 4);
		memcpy(&rval, rhs_data, 4);
		equal = (abs(lval - rval) < 0.001);
		greater = (lval > rval);
		less = (lval < rval);
	}

	bool match = false;
	switch(this->Operator) {
		case EQ_OP:
			match = equal;
			break;
		case GT_OP:
			match = greater;
			break;
		case GE_OP:
			match = equal || greater;
			break;
		case LT_OP:
			match = less;
			break;
		case LE_OP:
			match = equal || less;
			break;
		case NE_OP:
			match = !equal;
			break;
		default:
			cout << "matchCondition problem? check!" << endl;
			break;
	}
	return match;
}



Project::Project(Iterator *input, const vector<string> &attrNames) {
	input->getAttributes(this->allAttrs);
	this->inputIter = input;

	//configure projAttrs, projAttrs may have different order from allAttrs
	for(unsigned i = 0; i < attrNames.size(); i++) {
		for(unsigned j = 0; j < allAttrs.size(); j++) {
			//TODO: attribute prefix error!
			if(attrNames[i] == allAttrs[j].name) {
				this->projAttrs.push_back(allAttrs[j]);
				break;
			}
		}
	}

}

Project::~Project() {

}

RC Project::getNextTuple(void * data) {
	RC rc;
	void * buffer = calloc(PF_PAGE_SIZE, 1);

	rc = this->inputIter->getNextTuple(buffer);
	if(rc != 0) {
		//cout<<"There is no next tuple"<<endl;
		free(buffer);
		return -1;
	}

	void * value = calloc(PF_PAGE_SIZE, 1);
	int valueLength = 0, dataOffset = 0;
	for(unsigned i = 0; i < this->projAttrs.size(); i++) {
		string tempAttrName = projAttrs[i].name;
		getOneAttrValue(buffer, value, this->allAttrs, tempAttrName, &valueLength);
		memcpy((char*)data + dataOffset, value, valueLength);
		dataOffset += valueLength;
	}

	free(buffer);
	free(value);
	return 0;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->projAttrs;
	unsigned i;

//	for(i = 0; i < attrs.size(); i++) {
//		string temp = "ABCDEFG-Project"; //TODO:
//		temp += ".";
//		temp += this->projAttrs[i].name;
//		attrs[i].name = temp;
//	}
}

int myHash(void *data, string attrname, const vector<Attribute> &all_attrs, unsigned numParts) {
	// find type first
	AttrType type;
	bool found = false;
	int offset = 0;
	for (unsigned i = 0; i < all_attrs.size(); i++) {
		if (attrname == all_attrs[i].name) {
			type = all_attrs[i].type;
			found = true;
			break;
		}
		if (all_attrs[i].type == TypeVarChar) {
			int len = *(int*)((char*)data+offset);
			offset += (4+len);
		} else {
			offset += 4;
		}
	}
	if (!found) {
		printf("myHash can't find...check!\n");
	}

	int hashvalue = 0;
	if (type == TypeVarChar) {  //TODO: Notice how to deal with string
		int len = *(int*)((char*)data+offset);
		string str = string((char*)data+offset+4, len);
		for (unsigned i = 0; i < str.size(); i++) {
			hashvalue += str[i];
		}
		hashvalue %= numParts;
	} else if (type == TypeInt) {
		hashvalue = *(int*)((char*)data+offset) % numParts;
		//cout << *(int*)data << " " << hashvalue << endl;
	} else {
		hashvalue = (int)(*(float*)((char*)data+offset)) % numParts;
	}
	return hashvalue;
}


template <class T>
inline std::string to_string (const T& t)
{
	std::stringstream ss;
	ss << t;
	return ss.str();
}

void setRecNumber(void *part, int num) {
	memcpy((char*)part+PF_PAGE_SIZE-4, &num, 4);
}

int getRecNumber(void *part) {
	return *(int*)((char*)part+PF_PAGE_SIZE-4);
}

HashJoin::HashJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPages) {
	mkdir("Partitions", 0777);
	partitionDir = "Partitions/"+to_string(time(NULL));
	mkdir(partitionDir.c_str(), 0777);
	leftDir = partitionDir+"/left";
	mkdir(leftDir.c_str(), 0777);
	rightDir = partitionDir+"/right";
	mkdir(rightDir.c_str(), 0777);
	leftInput = leftIn;
	rightInput = rightIn;
	leftAttrName = condition.lhsAttr;
	rightAttrName = condition.rhsAttr;
	leftInput->getAttributes(leftAttrs);
	rightInput->getAttributes(rightAttrs);

	numPartitions = numPages - 1;
	PFM = PF_Manager::Instance();

	// init partitions
	vector<void*> partitions_buffer;
	vector<int> partitions_offset;
	for (unsigned i = 0; i < numPartitions; i++) {
		PFM->CreateFile((leftDir+"/"+to_string(i)).c_str());
		PFM->CreateFile((rightDir+"/"+to_string(i)).c_str());
		void *part = malloc(PF_PAGE_SIZE);
		setRecNumber(part, 0);
		partitions_buffer.push_back(part);
		partitions_offset.push_back(0);
	}

	void *data = malloc(PF_PAGE_SIZE);
	
	// left partitions
	while(leftInput->getNextTuple(data) != QE_EOF) {
		// get partition number
		int parnum = myHash(data, leftAttrName, leftAttrs, numPartitions);

		/* 
			insert record
			if has free space, then insert
			if not, write current partition, append a new page then insert
		*/

		// check free space
		int tuple_len = calculateTupleLength(data, leftAttrs);
		// if no free space
		if (partitions_offset[parnum] + tuple_len >= PF_PAGE_SIZE - 4) {
			PF_FileHandle fh;
			PFM->OpenFile((leftDir+"/"+to_string(parnum)).c_str(), fh);
			fh.AppendPage(partitions_buffer[parnum]);
			PFM->CloseFile(fh);

			setRecNumber(partitions_buffer[parnum], 0);
			partitions_offset[parnum] = 0;
		}
		// insert tuple
		memcpy((char*)partitions_buffer[parnum]+partitions_offset[parnum], data, tuple_len);
		partitions_offset[parnum] += tuple_len;

		int n = getRecNumber(partitions_buffer[parnum]);
		setRecNumber(partitions_buffer[parnum], n+1);
	}

	// flush & clean all left partitions
	for (unsigned i = 0; i < numPartitions; i++) {
		PF_FileHandle fh;
		PFM->OpenFile((leftDir+"/"+to_string(i)).c_str(), fh);
		fh.AppendPage(partitions_buffer[i]);
		PFM->CloseFile(fh);

		setRecNumber(partitions_buffer[i], 0);
		partitions_offset[i] = 0;
	}



	// right partitions
	while(rightInput->getNextTuple(data) != QE_EOF) {
		// get partition number
		int parnum = myHash(data, rightAttrName, rightAttrs, numPartitions);
		
		/* 
			insert record
			if has free space, then insert
			if not, write current partition, append a new page then insert
		*/

		// check free space
		int tuple_len = calculateTupleLength(data, rightAttrs);
		// if no free space
		if (partitions_offset[parnum] + tuple_len >= PF_PAGE_SIZE - 4) {
			PF_FileHandle fh;
			PFM->OpenFile((rightDir+"/"+to_string(parnum)).c_str(), fh);
			fh.AppendPage(partitions_buffer[parnum]);
			PFM->CloseFile(fh);

			setRecNumber(partitions_buffer[parnum], 0);
			partitions_offset[parnum] = 0;
		}
		// insert tuple
		memcpy((char*)partitions_buffer[parnum]+partitions_offset[parnum], data, tuple_len);
		partitions_offset[parnum] += tuple_len;

		int n = getRecNumber(partitions_buffer[parnum]);
		setRecNumber(partitions_buffer[parnum], n+1);
	}

	// flush & clean right partitions
	for (unsigned i = 0; i < numPartitions; i++) {
		PF_FileHandle fh;
		PFM->OpenFile((rightDir+"/"+to_string(i)).c_str(), fh);
		fh.AppendPage(partitions_buffer[i]);
		PFM->CloseFile(fh);

		setRecNumber(partitions_buffer[i], 0);
		partitions_offset[i] = 0;
	}

	// free buffer
	for (unsigned i = 0; i < numPartitions; i++) {
		free(partitions_buffer[i]);
	}

	free(data);

	this->currNumPartition = 0;
	this->started = false;
	for(unsigned i = 0; i < leftAttrs.size(); i++) {
		if(condition.lhsAttr == leftAttrs[i].name) {
			this->attrType = leftAttrs[i].type;
			break;
		}
	}

	//configure the joined attributes
	this->joinedAttrs = this->leftAttrs;
	for(unsigned i = 0; i < rightAttrs.size(); i++) {
		joinedAttrs.push_back(rightAttrs[i]);
	}
}

HashJoin::~HashJoin() {

}

void HashJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->joinedAttrs;
}

// remember to clean files in getNextTuple
// system("rm -r " + partitionDir);



void HashJoin::clearHashTable() {
	//free all the entries in hash_map
	if(this->attrType == TypeInt) {
		unordered_multimap<int, void*>::iterator umMapForIntIter;
		for(umMapForIntIter = this->umMapForInt.begin(); umMapForIntIter != this->umMapForInt.end(); umMapForIntIter++) {
			free(umMapForIntIter->second);
		}
		this->umMapForInt.clear();

	} else if(this->attrType == TypeReal) {
		unordered_multimap<float, void*>::iterator umMapForRealIter;
		for(umMapForRealIter = this->umMapForReal.begin(); umMapForRealIter != this->umMapForReal.end(); umMapForRealIter++) {
			free(umMapForRealIter->second);
		}
		this->umMapForReal.clear();
	} else if(this->attrType == TypeVarChar) {
		unordered_multimap<string, void*>::iterator umMapForStringIter;
		for(umMapForStringIter = this->umMapForString.begin(); umMapForStringIter != this->umMapForString.end(); umMapForStringIter++) {
			free(umMapForStringIter->second);
		}
		this->umMapForString.clear();
	}
}

void HashJoin::buildHashTable() {
	if(this->attrType == TypeInt) {
		//clear hash table, prepare for next partition
		this->clearHashTable();

		//read next partition and recreate hash_map
		string RiName = this->leftDir + "/" + to_string(this->currNumPartition);
		//ReadPartition readPartitionRi(RiName, this->leftAttrs);
		ReadPartition readPartitionRi;
		readPartitionRi.recreate(RiName, this->leftAttrs);

		unsigned tupleLen = 0;
		while(true) {
			void * tempTuple = calloc(PF_PAGE_SIZE, 1);
			RC rc = ::getNextPartitionRecord(tempTuple, tupleLen, readPartitionRi);
					//readPartitionRi->getNextPartitionRecord(tempTuple, tupleLen);
			if(rc == -1) {
				free(tempTuple);
				readPartitionRi.clear();
				break;
			} else {
				int key;
				int keyLength;
				::getOneAttrValue(tempTuple, &key, this->leftAttrs, this->leftAttrName, &keyLength);
				this->umMapForInt.insert(pair<int, void *> (key, tempTuple));
			}
		}
	}

	else if(this->attrType == TypeReal) {
		//clear hash table, prepare for next partition
		this->clearHashTable();

		//read next partition and recreate hash_map
		string RiName = this->leftDir + "/" + to_string(this->currNumPartition);
		//ReadPartition readPartitionRi(RiName, this->leftAttrs);
		ReadPartition readPartitionRi;
		readPartitionRi.recreate(RiName, this->leftAttrs);

		unsigned tupleLen = 0;
		while(true) {
			void * tempTuple = calloc(PF_PAGE_SIZE, 1);
			RC rc = ::getNextPartitionRecord(tempTuple, tupleLen, readPartitionRi);
					//readPartitionRi->getNextPartitionRecord(tempTuple, tupleLen);
			if(rc == -1) {
				free(tempTuple);
				readPartitionRi.clear();
				break;
			} else {
				float key;
				int keyLength;
				::getOneAttrValue(tempTuple, &key, this->leftAttrs, this->leftAttrName, &keyLength);
				this->umMapForReal.insert(pair<float, void *> (key, tempTuple));
			}
		}
	}

	else if(this->attrType == TypeVarChar) {
		//clear hash table, prepare for next partition
		this->clearHashTable();

		//read next partition and recreate hash_map
		string RiName = this->leftDir + "/" + to_string(this->currNumPartition);
		//ReadPartition readPartitionRi(RiName, this->leftAttrs);
		ReadPartition readPartitionRi;
		readPartitionRi.recreate(RiName, this->leftAttrs);

		unsigned tupleLen = 0;
		while(true) {
			void * tempTuple = calloc(PF_PAGE_SIZE, 1);
			RC rc = ::getNextPartitionRecord(tempTuple, tupleLen, readPartitionRi);
					//readPartitionRi->getNextPartitionRecord(tempTuple, tupleLen);
			if(rc == -1) {
				free(tempTuple);
				readPartitionRi.clear();
				break;
			} else {
				void* key = calloc(PF_PAGE_SIZE, 1); //TODO: Notice the handling with string
				int keyLength;
				::getOneAttrValue(tempTuple, key, this->leftAttrs, this->leftAttrName, &keyLength);
				string keyValue = string((char*)key + 4, keyLength);
				this->umMapForString.insert(pair<string, void *> (keyValue, tempTuple));
				free(key);
			}
		}
	}
}

RC HashJoin::nextProbing() {
	bool found = false;
	while(!found) {
		unsigned tempTupleSiLen;
		void * tempTupleSi = calloc(PF_PAGE_SIZE, 1);

		RC rc = getNextPartitionRecord(tempTupleSi, tempTupleSiLen, this->readPartitionSi);

		if(rc == -1) { //Si has scaned through
			//: recreate hashtable & read partition S i+1
			this->clearHashTable();
			this->readPartitionSi.clear();
			//delete this->readPartitionSi;

			if(this->currNumPartition == this->numPartitions) {
				system(("rm -r " + this->partitionDir).c_str());
				this->clearHashTable();
				return -1;
			} else {
				this->clearHashTable();
				this->buildHashTable();
				string SiName = this->rightDir + "/" + to_string(this->currNumPartition); //:configure Si Name
				//this->readPartitionSi = new ReadPartition(SiName, this->rightAttrs);
				this->readPartitionSi.recreate(SiName, this->rightAttrs);
			}
			this->currNumPartition++;
		} else {
			if(this->attrType == TypeInt) {
				int tempSKey;
				int tempSKeyLen;
				::getOneAttrValue(tempTupleSi, &tempSKey, this->rightAttrs, this->rightAttrName, &tempSKeyLen);

				unordered_multimap<int, void *>::iterator umMapForIntIter;
				pair<unordered_multimap<int, void *>::iterator, unordered_multimap<int, void *>::iterator> p
					= this->umMapForInt.equal_range(tempSKey);

				if(p.first == this->umMapForInt.end() && p.second == this->umMapForInt.end()) {
					free(tempTupleSi);	//no match hash values
				} else {
					for(umMapForIntIter = p.first; umMapForIntIter != p.second; umMapForIntIter++) {
						//
						void * tempBuffer = calloc(PF_PAGE_SIZE, 1);
						int tempBufferLen;
						mergeTuple(tempBufferLen, tempBuffer, umMapForIntIter->second, this->leftAttrs, tempTupleSi, this->rightAttrs);
						this->results.push(tempBuffer);
						this->resultsLen.push(tempBufferLen);
					}
					free(tempTupleSi);
					return 0;
				}
			}
			else if(this->attrType == TypeReal) {
				float tempSKey;
				int tempSKeyLen;
				::getOneAttrValue(tempTupleSi, &tempSKey, this->rightAttrs, this->rightAttrName, &tempSKeyLen);

				unordered_multimap<float, void *>::iterator umMapForRealIter;
				pair<unordered_multimap<float, void *>::iterator, unordered_multimap<float, void *>::iterator> p
					= this->umMapForReal.equal_range(tempSKey);

				if(p.first == this->umMapForReal.end() && p.second == this->umMapForReal.end()) {
					free(tempTupleSi);	//no match hash values
				} else {
					for(umMapForRealIter = p.first; umMapForRealIter != p.second; umMapForRealIter++) {
						//
						void * tempBuffer = calloc(PF_PAGE_SIZE, 1);
						int tempBufferLen;
						mergeTuple(tempBufferLen, tempBuffer, umMapForRealIter->second, this->leftAttrs, tempTupleSi, this->rightAttrs);
						this->results.push(tempBuffer);
						this->resultsLen.push(tempBufferLen);
					}
					//::mergeTuple(data, hMapForIntIter->second, this->leftAttrs, tempTupleSi, this->rightAttrs);
					free(tempTupleSi);
					return 0;
				}
			}
			else if(this->attrType == TypeVarChar) { //TODO:configure string value
				void * tempSKey = calloc(PF_PAGE_SIZE, 1);
				int tempSKeyLen;
				::getOneAttrValue(tempTupleSi, tempSKey, this->rightAttrs, this->rightAttrName, &tempSKeyLen);
				string tempSKeyValue = string((char*)tempSKey + 4, tempSKeyLen);

				unordered_multimap<string, void *>::iterator umMapForStringIter;
				pair<unordered_multimap<string, void *>::iterator, unordered_multimap<string, void *>::iterator> p
					= this->umMapForString.equal_range(tempSKeyValue);

				if(p.first == this->umMapForString.end() && p.second == this->umMapForString.end()) {
					free(tempTupleSi);	//no match hash values
					free(tempSKey);
				} else {
					for(umMapForStringIter = p.first; umMapForStringIter != p.second; umMapForStringIter++) {
						void * tempBuffer = calloc(PF_PAGE_SIZE, 1);
						int tempBufferLen;
						mergeTuple(tempBufferLen, tempBuffer, umMapForStringIter->second, this->leftAttrs, tempTupleSi, this->rightAttrs);
						this->results.push(tempBuffer);
						this->resultsLen.push(tempBufferLen);
					}
					free(tempSKey);
					free(tempTupleSi);
					return 0;
				}
			}
		}

	}

	system(("rm -r " + this->partitionDir).c_str());
	this->clearHashTable();
	return -1;
}

RC HashJoin::getNextTuple(void * data) {
	return this->probingPhase(data);
}

RC HashJoin::probingPhase(void *data) {
	if(!this->results.empty()) {
		void * temp = this->results.front();
		this->results.pop();
		int tempLen = this->resultsLen.front();
		this->resultsLen.pop();
		memcpy(data, temp, tempLen);
		free(temp);
		return 0;
	}

	if(!this->started) {
		this->clearHashTable();
		this->buildHashTable();

		string siName = this->rightDir + "/" + to_string(this->currNumPartition); //TODO: partition name configure
		//this->readPartitionSi = new ReadPartition(siName, this->rightAttrs);
		this->readPartitionSi.recreate(siName, this->rightAttrs);
		this->currNumPartition++;
		this->started = true;
	}

	RC rc = nextProbing();
	if(rc == -1)
		return -1;
	else {
		if(!this->results.empty()) {
			void * temp = this->results.front();
			this->results.pop();
			int tempLen = this->resultsLen.front();
			this->resultsLen.pop();
			memcpy(data, temp, tempLen);  //
			free(temp);
			return 0;
		}
	}

}





NLJoin::NLJoin(Iterator *leftIn, TableScan *rightIn,
        const Condition &condition, const unsigned numPages) {
	//TODO: check if bRhsIsAttr
	if(!condition.bRhsIsAttr) {
		cout<<"The condition cannot satisfy join"<<endl;

	}

	this->inputLeft = leftIn;
	this->inputRight = rightIn;
	this->lhsAttr = condition.lhsAttr;
	this->rhsAttr = condition.rhsAttr;
	this->op = condition.op;

	leftIn->getAttributes(this->leftAttrs);
	rightIn->getAttributes(this->rightAttrs);

	for(unsigned i = 0; i < leftAttrs.size(); i++) {
		if(lhsAttr == leftAttrs[i].name) {
			this->attrType = leftAttrs[i].type;
			break;
		}
	}

	//configure the joinedAttrs (combine the left and right)
	this->joinedAttrs = this->leftAttrs;
	for(unsigned i = 0; i < rightAttrs.size(); i++) {
		joinedAttrs.push_back(rightAttrs[i]);
	}

	this->tupleNum = 0;
	this->leftStarted = false;
	this->leftBuffer = calloc(PF_PAGE_SIZE, 1);
	this->rightBuffer = calloc(PF_PAGE_SIZE, 1);
}

NLJoin::~NLJoin() {
	free(this->leftBuffer);
	free(this->rightBuffer);
}

int compareAttrValue(const void *leftValue, const int leftLen,
		const void *rightValue, const int rightLen, AttrType attrType) {
	if(attrType == TypeInt) {
		int left, right;
		memcpy(&left, leftValue, sizeof(int));
		memcpy(&right, rightValue, sizeof(int));
		if(abs(left-right) < 0.001)
			return 0;
		if(left > right)
			return 1;
		if(left < right)
			return -1;
	} else if(attrType == TypeReal) {
		float left, right;
		memcpy(&left, leftValue, sizeof(float));
		memcpy(&right, rightValue, sizeof(float));
		if(left - right < 0.001 && left - right > -0.001)
			return 0;
		if(left > right)
			return 1;
		if(left < right)
			return -1;
	} else if(attrType == TypeVarChar) {
		unsigned leftStringLen, rightStringLen;
		memcpy(&leftStringLen, leftValue, sizeof(int));
		memcpy(&rightStringLen, rightValue, sizeof(int));
		string leftString = string((char*)leftValue + 4, leftStringLen);
		string rightString = string((char*)rightValue + 4, rightStringLen);

//		return strcmp(leftString.c_str(), rightString.c_str());
//
		if(leftString == rightString)
			return 0;
		if(leftString > rightString)
			return 1;
		if(leftString < rightString)
			return -1;
	}
}

RC NLJoin::getNextTupleInner(void *data, void *leftValue, int leftLen, void *rightValue, int rightLen) {
	bool found = false;
	RC rc;
	int dataLen;
	while(!found) {
		::getOneAttrValue(leftBuffer, leftValue, this->leftAttrs, this->lhsAttr, &leftLen);

		rc = this->inputRight->getNextTuple(rightBuffer);
		if(rc != 0) {
			rc = this->inputLeft->getNextTuple(leftBuffer);
			if(rc != 0) {
				//cout<<"There is no next tuple"<<endl;
				return -1;
			}
			::getOneAttrValue(leftBuffer, leftValue, this->leftAttrs, this->lhsAttr, &leftLen);

			this->inputRight->setIterator();

			rc = this->inputRight->getNextTuple(rightBuffer);
			if(rc != 0)
				continue;
		}
		::getOneAttrValue(rightBuffer, rightValue, this->rightAttrs, this->rhsAttr, &rightLen);

		if(this->op == EQ_OP) {
			if(compareAttrValue(leftValue, leftLen, rightValue, rightLen, this->attrType) == 0) {
				::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
				free(leftValue);
				free(rightValue);
				this->tupleNum++;
				return 0;
			}
		} else if(this->op == LT_OP) {
			if(compareAttrValue(leftValue, leftLen, rightValue, rightLen, this->attrType) < 0) {
				::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
				free(leftValue);
				free(rightValue);
				this->tupleNum++;
				return 0;
			}
		} else if(this->op == LE_OP) {
			if(compareAttrValue(leftValue, leftLen, rightValue, rightLen, this->attrType) <= 0) {
				::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
				free(leftValue);
				free(rightValue);
				this->tupleNum++;
				return 0;
			}
		} else if(this->op == GT_OP) {
			if(compareAttrValue(leftValue, leftLen, rightValue, rightLen, this->attrType) > 0) {
				::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
				free(leftValue);
				free(rightValue);
				this->tupleNum++;
				return 0;
			}
		} else if(this->op == GE_OP) {
			if(compareAttrValue(leftValue, leftLen, rightValue, rightLen, this->attrType) >= 0) {
				::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
				free(leftValue);
				free(rightValue);
				this->tupleNum++;
				return 0;
			}
		} else if(this->op == NE_OP) {
			if(compareAttrValue(leftValue, leftLen, rightValue, rightLen, this->attrType) != 0) {
				::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
				free(leftValue);
				free(rightValue);
				this->tupleNum++;
				return 0;
			}
		} else if(this->op == NO_OP) {
			::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
			free(leftValue);
			free(rightValue);
			this->tupleNum++;
			return 0;
		}

	}

}

RC NLJoin::getNextTuple(void *data) {
	RC rc;
	void * leftValue = calloc(PF_PAGE_SIZE, 1);
	void * rightValue = calloc(PF_PAGE_SIZE, 1);
	int leftLen, rightLen;

	if(this->leftStarted == false) {
		this->inputLeft->getNextTuple(this->leftBuffer);
		::getOneAttrValue(leftBuffer, leftValue, this->leftAttrs, this->lhsAttr, &leftLen);
		this->leftStarted = true;
	}

	if(this->inputRight->iter == NULL) {
		CompOp newOp;
		if(this->op == LT_OP) newOp = GT_OP;
		else if(this->op == LE_OP) newOp = GE_OP;
		else if(this->op == GT_OP) newOp = LT_OP;
		else if(this->op == GE_OP) newOp = LE_OP;
		else newOp = this->op;
		this->inputRight->setIterator();
	}

	rc = getNextTupleInner(data, leftValue, leftLen, rightValue, rightLen);

//	free(leftValue);
//	free(rightValue);
	return rc;
}

void NLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->joinedAttrs;
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn,
        const Condition &condition, const unsigned numPages) {
	//TODO: check if bRhsIsAttr
	if(!condition.bRhsIsAttr) {
		cout<<"The condition cannot satisfy join operation! ERROR!!!"<<endl;

	}

	this->inputLeft = leftIn;
	this->inputRight = rightIn;
	this->lhsAttr = condition.lhsAttr;
	this->rhsAttr = condition.rhsAttr;
	this->op = condition.op;

	leftIn->getAttributes(this->leftAttrs);
	rightIn->getAttributes(this->rightAttrs);

	for(unsigned i = 0; i < leftAttrs.size(); i++) {
		if(lhsAttr == leftAttrs[i].name) {
			this->attrType = leftAttrs[i].type;
			break;
		}
	}

	//configure the joinedAttrs (combine the left and right)
	this->joinedAttrs = this->leftAttrs;
	for(unsigned i = 0; i < rightAttrs.size(); i++) {
		joinedAttrs.push_back(rightAttrs[i]);
	}

	this->tupleNum = 0;
	this->leftStarted = false;
	this->leftBuffer = calloc(PF_PAGE_SIZE, 1);
	this->rightBuffer = calloc(PF_PAGE_SIZE, 1);
}

INLJoin::~INLJoin() {
	free(this->leftBuffer);
	free(this->rightBuffer);
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = this->joinedAttrs;
}

RC INLJoin::getNextTupleInner(void *data, void *leftValue, int leftLen,
		void *rightValue, int rightLen) {
	bool found = false;
	RC rc;
	int dataLen;
	while(!found) {
		::getOneAttrValue(leftBuffer, leftValue, this->leftAttrs, this->lhsAttr, &leftLen);

		rc = this->inputRight->getNextTuple(rightBuffer);
		if(rc != 0) {
			rc = this->inputLeft->getNextTuple(leftBuffer);
			if(rc != 0) {
				//cout<<"There is no next tuple"<<endl;
				return -1;
			}
			::getOneAttrValue(leftBuffer, leftValue, this->leftAttrs, this->lhsAttr, &leftLen);

			CompOp newOp;
			if(this->op == LT_OP) newOp = GT_OP;
			else if(this->op == LE_OP) newOp = GE_OP;
			else if(this->op == GT_OP) newOp = LT_OP;
			else if(this->op == GE_OP) newOp = LE_OP;
			else newOp = this->op;
			this->inputRight->setIterator(newOp, leftValue);

			rc = this->inputRight->getNextTuple(rightBuffer);
			if(rc != 0)
				continue;
		}
		::getOneAttrValue(rightBuffer, rightValue, this->rightAttrs, this->rhsAttr, &rightLen);

		if(this->op == EQ_OP) {
			if(leftLen == rightLen && memcmp(leftValue, rightValue, rightLen) == 0) {
				::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
				free(leftValue);
				free(rightValue);
				this->tupleNum++;
				return 0;
			}
		} else if(this->op == LT_OP) {
			if(compareAttrValue(leftValue, leftLen, rightValue, rightLen, this->attrType) < 0) {
				::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
				free(leftValue);
				free(rightValue);
				this->tupleNum++;
				return 0;
			}
		} else if(this->op == LE_OP) {
			if(compareAttrValue(leftValue, leftLen, rightValue, rightLen, this->attrType) <= 0) {
				::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
				free(leftValue);
				free(rightValue);
				this->tupleNum++;
				return 0;
			}
		} else if(this->op == GT_OP) {
			if(compareAttrValue(leftValue, leftLen, rightValue, rightLen, this->attrType) > 0) {
				::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
				free(leftValue);
				free(rightValue);
				this->tupleNum++;
				return 0;
			}
		} else if(this->op == GE_OP) {
			if(compareAttrValue(leftValue, leftLen, rightValue, rightLen, this->attrType) >= 0) {
				::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
				free(leftValue);
				free(rightValue);
				this->tupleNum++;
				return 0;
			}
		} else if(this->op == NE_OP) {
			if(compareAttrValue(leftValue, leftLen, rightValue, rightLen, this->attrType) != 0) {
				::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
				free(leftValue);
				free(rightValue);
				this->tupleNum++;
				return 0;
			}
		} else if(this->op == NO_OP) {
			::mergeTuple(dataLen, data, leftBuffer, this->leftAttrs, rightBuffer, this->rightAttrs);
			free(leftValue);
			free(rightValue);
			this->tupleNum++;
			return 0;
		}

	}

}

RC INLJoin::getNextTuple(void * data) {
	RC rc;
	void * leftValue = calloc(PF_PAGE_SIZE, 1);
	void * rightValue = calloc(PF_PAGE_SIZE, 1);
	int leftLen = 0, rightLen = 0;

	if(this->leftStarted == false) {
		this->inputLeft->getNextTuple(this->leftBuffer);
		::getOneAttrValue(leftBuffer, leftValue, this->leftAttrs, this->lhsAttr, &leftLen);
		this->leftStarted = true;
	}

	if(this->inputRight->iter == NULL) {
		CompOp newOp;
		if(this->op == LT_OP) newOp = GT_OP;
		else if(this->op == LE_OP) newOp = GE_OP;
		else if(this->op == GT_OP) newOp = LT_OP;
		else if(this->op == GE_OP) newOp = LE_OP;
		else newOp = this->op;
		this->inputRight->setIterator(newOp, leftValue);
	}

	rc = getNextTupleInner(data, leftValue, leftLen, rightValue, rightLen);

//	free(leftValue);
//	free(rightValue);
	return rc;
}

AggrCon::AggrCon() {
	this->avg = 0.0;
	this->sum = 0.0;
	this->count = 0.0;
	this->min = 3.40282346638528859812e+38F;
	this->max = 1.17549435082228750797e-38F;
	//float min = 3.40282346638528859812e+38F;
	//float max = 1.17549435082228750797e-38F ;
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
	this->iterIn = input;
	this->aggAttr = aggAttr;
	this->op = op;
	input->getAttributes(this->inAttrs);


	this->isGrouped = false;
	this->scaned = false;
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr,
          Attribute gAttr, AggregateOp op ) {
	this->iterIn = input;
	this->aggAttr = aggAttr;
	this->gAttr = gAttr;
	this->op = op;
	input->getAttributes(this->inAttrs);

	this->isGrouped = true;
	this->scaned = false;
}

Aggregate::~Aggregate() {

}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {
	if(!this->isGrouped) {
		Attribute attr;
		for(unsigned i = 0; i < this->inAttrs.size(); i++) {
			if(inAttrs[i].name == this->aggAttr.name) {
				attr = inAttrs[i];
				break;
			}
		}
		string prefix;
		if(this->op == MIN)
			prefix = "MIN";
		else if(this->op == MAX)
			prefix = "MAX";
		else if(this->op == SUM)
			prefix = "SUM";
		else if(this->op == AVG)
			prefix = "AVG";
		else if(this->op == COUNT)
			prefix = "COUNT";

		attr.name = prefix + "(" + attr.name + ")";
		attr.type = TypeReal;
		attrs.clear();
		attrs.push_back(attr);
	} else {  //Grouped
		//TODO:name the new Attrubute after project
		Attribute attr;
		for(unsigned i = 0; i < this->inAttrs.size(); i++) {
			if(inAttrs[i].name == this->aggAttr.name) {
				attr = inAttrs[i];
				break;
			}
		}
		string prefix;
		if(this->op == MIN)
			prefix = "MIN";
		else if(this->op == MAX)
			prefix = "MAX";
		else if(this->op == SUM)
			prefix = "SUM";
		else if(this->op == AVG)
			prefix = "AVG";
		else if(this->op == COUNT)
			prefix = "COUNT";

		attr.name = prefix + "(" + attr.name + ")";
		attr.type = TypeReal;

		attrs.clear();
		attrs.push_back(this->gAttr);
		attrs.push_back(attr);
	}
}

RC Aggregate::nextTupleNotGrouped(void *data) {
	float count = 0, sum = 0, avg;
	float min = 3.40282346638528859812e+38F;
	float max = 1.17549435082228750797e-38F ;

	if(scaned) return -1;

	void * buffer = calloc(PF_PAGE_SIZE, 1);
	void * value = calloc(PF_PAGE_SIZE, 1);
	int valueLen;

	while(this->iterIn->getNextTuple(buffer) != QE_EOF) {
		::getOneAttrValue(buffer, value, this->inAttrs, this->aggAttr.name, &valueLen);
		float valueKey;
		if(this->aggAttr.type == TypeInt) {
			int temp;
			memcpy(&temp, value, sizeof(int));
			valueKey = (float)temp;
		} else if(this->aggAttr.type == TypeReal) {
			memcpy(&valueKey, value, sizeof(float));
		} else {
			cout<<"Error!! Aggregate can not take string value!"<<endl;
			return -1;
		}

		count++;
		sum += valueKey;
		if(max < valueKey) max = valueKey;
		if(min > valueKey) min = valueKey;
	}
	avg = sum / count;

	if(this->op == SUM) {
		memcpy(data, &sum, sizeof(float));
	} else if(this->op == COUNT) {
		memcpy(data, &count, sizeof(float));
	} else if(this->op == MIN) {
		memcpy(data, &min, sizeof(float));
	} else if(this->op == MAX) {
		memcpy(data, &max, sizeof(float));
	} else if(this->op == AVG) {
		memcpy(data, &avg, sizeof(float));
	}

	free(buffer);
	free(value);
	scaned = true;
	return 0;
}

RC Aggregate::groupedAggregateScan() {
	void * tupleBuffer = calloc(PF_PAGE_SIZE, 1);
	void * aggAttrValue = calloc(PF_PAGE_SIZE, 1);
	void * gAttrValue = calloc(PF_PAGE_SIZE, 1);
	int aggAttrValueLen, gAttrValueLen;

	while(this->iterIn->getNextTuple(tupleBuffer) != QE_EOF) {
		::getOneAttrValue(tupleBuffer, aggAttrValue, this->inAttrs, this->aggAttr.name, &aggAttrValueLen);
		::getOneAttrValue(tupleBuffer, gAttrValue, this->inAttrs, this->gAttr.name, &gAttrValueLen);


		//get the key value of aggAttr
		float aggAttrValueKey;
		if(this->aggAttr.type == TypeInt) {
			int temp;
			memcpy(&temp, aggAttrValue, sizeof(int));
			aggAttrValueKey = (float)temp;
		} else if(this->aggAttr.type == TypeReal) {
			memcpy(&aggAttrValueKey, aggAttrValue, sizeof(float));
		} else {
			cout<<"Error!! Aggregate can not take string value!"<<endl;
			return -1;
		}

		if(this->gAttr.type == TypeInt) {
			//analyze the gAttr
			int gAttrValueKey;
			memcpy(&gAttrValueKey, gAttrValue, sizeof(int));

			unordered_map<int, AggrCon>::iterator iter = this->gMapForInt.find(gAttrValueKey);
			if(iter == this->gMapForInt.end()) {
				AggrCon tempAggrCon;
				tempAggrCon.count = 1;
				tempAggrCon.sum = aggAttrValueKey;
				tempAggrCon.max = aggAttrValueKey;
				tempAggrCon.min = aggAttrValueKey;
				tempAggrCon.avg = tempAggrCon.sum / tempAggrCon.count;
				gMapForInt.insert(pair<int, AggrCon> (gAttrValueKey, tempAggrCon));
			} else {
				iter->second.count++;
				iter->second.sum += aggAttrValueKey;
				if(aggAttrValueKey > iter->second.max) iter->second.max = aggAttrValueKey;
				if(aggAttrValueKey < iter->second.min) iter->second.min = aggAttrValueKey;
				iter->second.avg = iter->second.sum / iter->second.count;
			}
		} else if(this->gAttr.type == TypeReal) {
			//analyze the gAttr
			float gAttrValueKey;
			memcpy(&gAttrValueKey, gAttrValue, sizeof(float));

			unordered_map<float, AggrCon>::iterator iter = this->gMapForFloat.find(gAttrValueKey);
			if(iter == this->gMapForFloat.end()) {
				AggrCon tempAggrCon;
				tempAggrCon.count = 1;
				tempAggrCon.sum = aggAttrValueKey;
				tempAggrCon.max = aggAttrValueKey;
				tempAggrCon.min = aggAttrValueKey;
				tempAggrCon.avg = tempAggrCon.sum / tempAggrCon.count;
				gMapForFloat.insert(pair<float, AggrCon> (gAttrValueKey, tempAggrCon));
			} else {
				iter->second.count++;
				iter->second.sum += aggAttrValueKey;
				if(aggAttrValueKey > iter->second.max) iter->second.max = aggAttrValueKey;
				if(aggAttrValueKey < iter->second.min) iter->second.min = aggAttrValueKey;
				iter->second.avg = iter->second.sum / iter->second.count;
			}
		} else if(this->gAttr.type == TypeVarChar) {
			//analyze the gAttr, notice string
			int gAttrValueKeyLen;
			memcpy(&gAttrValueKeyLen, gAttrValue, sizeof(int));
			string gAttrValueKey = string((char*)gAttrValue + 4, gAttrValueKeyLen);

			unordered_map<string, AggrCon>::iterator iter = this->gMapForString.find(gAttrValueKey);
			if(iter == this->gMapForString.end()) {
				AggrCon tempAggrCon;
				tempAggrCon.count = 1;
				tempAggrCon.sum = aggAttrValueKey;
				tempAggrCon.max = aggAttrValueKey;
				tempAggrCon.min = aggAttrValueKey;
				tempAggrCon.avg = tempAggrCon.sum / tempAggrCon.count;
				gMapForString.insert(pair<string, AggrCon> (gAttrValueKey, tempAggrCon));
			} else {
				iter->second.count++;
				iter->second.sum += (float)aggAttrValueKey;
				if(aggAttrValueKey > iter->second.max) iter->second.max = aggAttrValueKey;
				if(aggAttrValueKey < iter->second.min) iter->second.min = aggAttrValueKey;
				iter->second.avg = iter->second.sum / iter->second.count;
			}
		}
	}

	//set iterator
	if(this->gAttr.type == TypeInt) {
		this->iterForIntMap = this->gMapForInt.begin();
	} else if(this->gAttr.type == TypeReal) {
		this->iterForRealMap = this->gMapForFloat.begin();
	} else if(this->gAttr.type == TypeVarChar) {
		this->iterForStringMap = this->gMapForString.begin();
	}

	free(tupleBuffer);
	free(aggAttrValue);
	free(gAttrValue);
}

bool Aggregate::groupedMapEnd() {
	if(this->gAttr.type == TypeInt) {
		if(this->iterForIntMap == this->gMapForInt.end())
			return true;
		else
			return false;
	} else if(this->gAttr.type == TypeReal) {
		if(this->iterForRealMap == this->gMapForFloat.end())
			return true;
		else
			return false;
	} else if(this->gAttr.type == TypeVarChar) {
		if(this->iterForStringMap == this->gMapForString.end())
			return true;
		else
			return false;
	}
}

RC Aggregate::nextTupleGrouped(void *data) {
	if(!this->scaned) {
		this->groupedAggregateScan();
		this->scaned = true;
	}

	if(this->groupedMapEnd()) {
//		cout<<"There is no next tuple"<<endl;
		return -1;
	}

	//has next tuple
	if(this->gAttr.type == TypeInt) {
		int g = this->iterForIntMap->first;
		float result;
		if(this->op == SUM) {
			result = iterForIntMap->second.sum;
		} else if(this->op == COUNT) {
			result = iterForIntMap->second.count;
		} else if(this->op == MIN) {
			result = iterForIntMap->second.min;
		} else if(this->op == MAX) {
			result = iterForIntMap->second.max;
		} else if(this->op == AVG) {
			result = iterForIntMap->second.avg;
		}

		memcpy(data, &g, sizeof(int));
		memcpy((char*)data + sizeof(int), &result, sizeof(float));
		this->iterForIntMap++;
		return 0;
	} else if(this->gAttr.type == TypeReal) {
		float g = this->iterForRealMap->first;
		float result;
		if(this->op == SUM) {
			result = iterForRealMap->second.sum;
		} else if(this->op == COUNT) {
			result = iterForRealMap->second.count;
		} else if(this->op == MIN) {
			result = iterForRealMap->second.min;
		} else if(this->op == MAX) {
			result = iterForRealMap->second.max;
		} else if(this->op == AVG) {
			result = iterForRealMap->second.avg;
		}

		memcpy(data, &g, sizeof(float));
		memcpy((char*)data + sizeof(float), &result, sizeof(float));
		this->iterForRealMap++;
		return 0;
	} else if(this->gAttr.type == TypeVarChar) {
		string g = this->iterForStringMap->first;
		float result;
		if(this->op == SUM) {
			result = iterForStringMap->second.sum;
		} else if(this->op == COUNT) {
			result = iterForStringMap->second.count;
		} else if(this->op == MIN) {
			result = iterForStringMap->second.min;
		} else if(this->op == MAX) {
			result = iterForStringMap->second.max;
		} else if(this->op == AVG) {
			result = iterForStringMap->second.avg;
		}

		int gLen = g.size();
		memcpy(data, &gLen, 4);
		//memcpy((char*)data + 4, &g, sizeof(float));
		memcpy((char*)data + 4, g.c_str(), gLen);
		//memcpy((char*)data + 8, &result, sizeof(float));
		memcpy((char*)data + 4 + gLen, &result, sizeof(float));
		this->iterForStringMap++;
		return 0;
	}

	return -1;
}


RC Aggregate::getNextTuple(void *data) {
	if(!this->isGrouped) {
		return nextTupleNotGrouped(data);
	}
	else {
		return nextTupleGrouped(data);
	}
}

void ReadPartition::recreate(const string partitionName, vector<Attribute> &attributes) {
    this->PFM = PFM->Instance();
    this->PFM->OpenFile(partitionName.c_str(), this->partitionHandle);
    this->attrs = attributes;

    this->pageNum = partitionHandle.GetNumberOfPages();
    //this->tupleNumOfPage = 0;
    this->currPageNum = 0;
    this->currTupleNum = 0;
    this->currPosition = 0;

    this->pageBuffer = calloc(PF_PAGE_SIZE, 1);
    this->partitionHandle.ReadPage(this->currPageNum, pageBuffer);

    int tempInt;
    memcpy(&tempInt, (char*)pageBuffer + PF_PAGE_SIZE - 4, sizeof(int));
    this->tupleNumOfPage = (unsigned) tempInt;
}

ReadPartition::ReadPartition()  {

}

void ReadPartition::clear() {
	free(this->pageBuffer);
	this->PFM->CloseFile(this->partitionHandle);
}

ReadPartition::~ReadPartition(){

}



void* ReadPartition::getNextRecord(unsigned &recordLength){
    while(true) {
        //Reading tuples within curr page
        while(this->currTupleNum < this->tupleNumOfPage) {
            unsigned len = this->nextRecordLength();
            void * result = calloc(len, 1);
            memcpy(result, (char*)this->pageBuffer + this->currPosition, len);
            this->currPosition += len;
            this->currTupleNum++;
            recordLength = len;
            return result;
        }
        //Have already read all the tuples in curr page
        if(this->currTupleNum == this->tupleNumOfPage) {
            this->currPageNum++;
            if(this->currPageNum == this->pageNum) {
                recordLength = 0;
                return NULL;
            } else {
                this->partitionHandle.ReadPage(this->currPageNum, this->pageBuffer);
                this->currPageNum++;
                this->currTupleNum = 0;
            }
        }
    }
    return NULL;
}

unsigned ReadPartition::nextRecordLength() {
    unsigned length = 0;
    unsigned currPos = this->currPosition;
    for(unsigned j = 0; j < this->attrs.size(); j++) {
        Attribute attr = this->attrs[j];
        if (attr.type == TypeVarChar) {
            length += (4 + *(unsigned*)((char*)this->pageBuffer + currPos));
            currPos += *(unsigned*)((char*)this->pageBuffer + currPos);
        } else {
        	currPos += 4;
            length += 4;
        }
    }
    return length;
}
