
#include "rm.h"

// For mkdir
#include <sys/stat.h>
#include <sys/types.h>

#include <iostream>
#include <fstream>
#include <cassert>
#include <cmath>

#include <errno.h>


RC success = 0;
RM* RM::_rm = 0;


void *formatData(const vector<Attribute> &attrs, const void *data, short *length) {
	int off = 0, data_len = 0;
	for (unsigned i = 0; i < attrs.size(); i++) {
		if (attrs[i].type == TypeVarChar) {
			int len;
			memcpy(&len, (char*)data+off, 4);
			data_len += len;
			off += 4 + len;
		} else {
			data_len += 4;
			off += 4;
		}
		data_len += 2;
	}
	*length = (short)data_len+2+2; // 2 for schema, 2 for last offset
	void *formateddata = malloc(data_len+2+2);
	short sn = 0;
	memcpy(formateddata, &sn, 2);
	off = 0;
	short recordoff = 2 + 2*((short)attrs.size()+1); // offset after directory of offsets
	for (unsigned i = 0; i < attrs.size(); i++) {
		memcpy((char*)formateddata + 2 + 2*i, &recordoff, 2);
		if (attrs[i].type == TypeVarChar) {
			int len;
			memcpy(&len, (char*)data+off, 4);
			memcpy((char*)formateddata + (int)recordoff, (char*)data+off+4, len);
			recordoff += (short)len;
			off += len;
		} else {
			memcpy((char*)formateddata + (int)recordoff, (char*)data+off, 4);
			recordoff += 4;
		}
		off += 4;
	}
	// indicating the last offset
	memcpy((char*)formateddata + 2 + 2*(short)attrs.size(), &recordoff, 2);
	return formateddata;
}

void *getTheIthAttr(int i, const vector<Attribute> &attrs, const void *formated, short *length) {
	if (i >= (int)attrs.size()) printf("Out of boundary!!\n");

	// remember to skip schema number
	short attroffset, attrnextoffset;
	memcpy(&attroffset, (char*)formated+2+i*2, 2);
	memcpy(&attrnextoffset, (char*)formated+2+(i+1)*2, 2);
	short len = attrnextoffset - attroffset;
	//printf("%d\n", len);
	if (attrs[i].type == TypeVarChar) {
		len = len + 4;
	}
	void *attrdata = malloc((int)len);
	
	// format to the "original" format
	if (attrs[i].type == TypeVarChar) {
		int tmp = (short)len - 4;
		memcpy(attrdata, &tmp, 4); 
		memcpy((char*)attrdata+4, (char*)formated+(int)attroffset, tmp);
	} else {
		memcpy(attrdata, (char*)formated+(int)attroffset, 4);
	}
	*length = (int)len;
	return attrdata;
}

// this is for new version of scan
void RM_ScanIterator::fetchRecord(void *buffer, vector<int> indexes, vector<Attribute> &attrs, void *formated, short *length) {
	vector<void*> tmp_data;
	vector<int> tmp_lens;
	short total_len = 0;
	//printf("%d\n", indexes.size());
	for (unsigned i = 0; i < attrs.size(); i++) {
		//printf("%d\n", i);
		bool include = false;
		for (unsigned j = 0; j < indexes.size(); j++) {
			if (indexes[j] == (int)i) {
				include = true;
				break;
			}
		}
		if (include) {
			short this_len;
			//printf("here\n");
			tmp_data.push_back(getTheIthAttr(i, attrs, formated, &this_len));
			tmp_lens.push_back((int)this_len);
			total_len += this_len;
		}
	}
	//printf("here");
	int nowlen = 0;
	for (unsigned i = 0; i < tmp_data.size(); i++) {
		memcpy((char*)buffer+nowlen, tmp_data[i], tmp_lens[i]);
		nowlen += tmp_lens[i];
	}
	*length = nowlen;

	// free memory
	for (unsigned i = 0; i < tmp_data.size(); i++) {
		free(tmp_data[i]);
	}
	return;
}



void *getOriginalFormat(const vector<int> indexes, const vector<Attribute> &attrs, const void *formated, short *length) {
	vector<void*> tmp_data;
	vector<int> tmp_lens;
	short total_len = 0;
	for (unsigned i = 0; i < attrs.size(); i++) {
		bool include = false;
		for (unsigned j = 0; j < indexes.size(); j++) {
			if (indexes[j] == (int)i) {
				include = true;
				break;
			}
		}
		if (include) {
			short this_len;
			tmp_data.push_back(getTheIthAttr(i, attrs, formated, &this_len));
			tmp_lens.push_back((int)this_len);
			total_len += this_len;
		}
	}

	void *original = malloc((int)total_len);
	int nowlen = 0;
	for (unsigned i = 0; i < tmp_data.size(); i++) {
		memcpy((char*)original+nowlen, tmp_data[i], tmp_lens[i]);
		nowlen += tmp_lens[i];
	}
	*length = nowlen;

	// free memory
	for (unsigned i = 0; i < tmp_data.size(); i++) {
		free(tmp_data[i]);
	}
	return original;
}


RM* RM::Instance()
{
    if(!_rm)
        _rm = new RM();
    
    return _rm;
}

RM::RM()
{
	RC rc;
	PFM = PF_Manager::Instance();
	TABLEDIR = "Tables";
	mkdir(TABLEDIR.c_str(), 0777);
	CATALOG_NAME = "Catalog";
	string catarec = CATALOG_NAME+".rec", catamap = CATALOG_NAME+".map";
	
	// don't have to check rc
	PFM->CreateFile(catarec.c_str());
	PFM->CreateFile(catamap.c_str());

	rc = PFM->OpenFile(catarec.c_str(), CataRecHandle);
	if (rc != success) {
		//printf("OpenFile in RM constructor error\n");
		exit(-1);	
	}

	// !!tmp for findpage
	//rc = initNewPage(CataRecHandle); // !!!!
	// !!tmp for findpage

	rc = PFM->OpenFile(catamap.c_str(), CataMapHandle);
	if (rc != success) {
		//printf("OpenFile in RM constructor error\n");
		exit(-1);	
	}

	void *page = calloc(PF_PAGE_SIZE, 1);
	rc = CataMapHandle.AppendPage(page);
	if (rc != success) {
		//printf("AppendPage in RM constructor error\n");
		exit(-1);
	}
	free(page);

	rc = PFM->CloseFile(CataMapHandle);
	if (rc != success) {
		//printf("CloseFile in RM constructor error\n");
		exit(-1);		
	}

	// hardcode catalog attributes
	Attribute attr;
	attr.name = "TableName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)1000;
	Cata_attrs.push_back(attr);
	Cata_attr_names.push_back(attr.name);
	
	attr.name = "ColName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)1000;
	Cata_attrs.push_back(attr);
	Cata_attr_names.push_back(attr.name);
	
	attr.name = "Type";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	Cata_attrs.push_back(attr);
	Cata_attr_names.push_back(attr.name);

	attr.name = "Length";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	Cata_attrs.push_back(attr);
	Cata_attr_names.push_back(attr.name);

	//!!
	attr.name = "Schema";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	Cata_attrs.push_back(attr);
	Cata_attr_names.push_back(attr.name);

	attr.name = "PageFileName";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)1000;
	Cata_attrs.push_back(attr);
	Cata_attr_names.push_back(attr.name);	

}

RM::~RM()
{
	RC rc;
	rc = PFM->CloseFile(CataRecHandle);
	if (rc != success) {
		//printf("CloseFile in RM destructor error\n");
		exit(-1);		
	}
	// ignore this
	// rc = PFM->CloseFile(CataMapHandle);
	// if (rc != success) {
	// 	printf("CloseFile in RM destructor error\n");
	// 	exit(-1);		
	// }
}

RC RM::createTable(const string tableName, const vector<Attribute> &attrs) {
	RC rc;

	// check whether this table exists or not
	vector<string> attributeNames;
	attributeNames.push_back("TableName");
	RM_ScanIterator rmsi;
	int tmp = tableName.length();	
	void *forscanvalue = malloc(4+tmp);
	memcpy(forscanvalue, &tmp, 4);
	memcpy((char*)forscanvalue+4, tableName.c_str(), tmp);
	rc = scanFromFile("TableName", EQ_OP, forscanvalue, attributeNames, rmsi, Cata_attrs, CATALOG_NAME+".rec");
	assert(rc == success);
	free(forscanvalue);
	if (rc != success) {
		//printf("scanFromFile in createTable error\n");
		return -1;			
	}
	if (rmsi.getNumOfTuples() != 0) {
		//printf("Table %s already exists.\n", tableName.c_str());
		return -1;
	}
	rmsi.close();

	string recfile = TABLEDIR+"/"+tableName+".rec", mapfile = TABLEDIR+"/"+tableName+".map";

	// insert attrs to catalog
	for (unsigned i = 0; i < attrs.size(); i++) {
		// count attr length
		int len = (4 + tableName.length()) + (4 + attrs[i].name.length()) + 4 + 4 + 4 + (4 + recfile.length());
		// truncate attr
		void *data = malloc(len);
		int off = 0;
		// tablename
		int val = tableName.length();
		memcpy((char *)data+off, &val, 4);
		off += 4;
		memcpy((char *)data+off, tableName.c_str(), tableName.length());		
		off += tableName.length();
		// attrname
		val = attrs[i].name.length();
		memcpy((char *)data+off, &val, 4);
		off += 4;
		memcpy((char *)data+off, attrs[i].name.c_str(), attrs[i].name.length());
		off += attrs[i].name.length();
		// attrtype
		val = (int)attrs[i].type;
		memcpy((char *)data+off, &val, 4);
		off += 4;
		// attrlength
		val = (int)attrs[i].length;
		memcpy((char *)data+off, &val, 4);
		off += 4;
		// schema
		val = 0;
		memcpy((char *)data+off, &val, 4);
		off += 4;
		// pagefilename
		val = recfile.length();
		memcpy((char *)data+off, &val, 4);
		off += 4;		
		memcpy((char *)data+off, recfile.c_str(), recfile.length());


		RID rid;
		string cataMap = CATALOG_NAME+".map";
		rc = insertTupleToFile(CataRecHandle, cataMap, data, rid, Cata_attrs);
		if (rc != success) {
			//printf("insertTupleToFile in createTable error\n");
			return -1;			
		}
	}

	// create record & bitmap file
	rc = PFM->CreateFile(recfile.c_str());
	if (rc != success) {
		//printf("CreateFile in createTable error\n");
		return -1;
	}
	rc = PFM->CreateFile(mapfile.c_str());
	if (rc != success) {
		//printf("CreateFile in createTable error\n");
		return -1;
	}

	// !! tmp for findpage
	// PF_FileHandle recHandle;
	// rc = PFM->OpenFile(recfile.c_str(), recHandle);
	// if (rc != success) {
	// 	printf("OpenFile in createTable error\n");
	// 	return -1;
	// }
	// rc = initNewPage(recHandle); // !!	
	// rc = PFM->CloseFile(recHandle);
	// if (rc != success) {
	// 	printf("CloseFile in createTable error\n");
	// 	return -1;
	// }
	// !! tmp for findpage

	// init bitmap
	PF_FileHandle fileHandle;
	rc = PFM->OpenFile(mapfile.c_str(), fileHandle);
	if (rc != success) {
		//printf("OpenFile in createTable error\n");
		return -1;
	}
	void *page = calloc(PF_PAGE_SIZE, 1);
	rc = fileHandle.AppendPage(page);
	free(page);
	if (rc != success) {
		//printf("appendPage in createTable error\n");
		return -1;
	}
	rc = PFM->CloseFile(fileHandle);
	if (rc != success) {
		//printf("CloseFile in createTable error\n");
		return -1;
	}
	return 0;
}

RC RM::deleteTable(const string tableName) {
	RC rc;

	// check catalog if this table exists
	vector<string> attributeNames;
	attributeNames.push_back("TableName");
	RM_ScanIterator rmsi;
	int tmp = tableName.length();	
	void *forscanvalue = malloc(4+tmp);
	memcpy(forscanvalue, &tmp, 4);
	memcpy((char*)forscanvalue+4, tableName.c_str(), tmp);
	rc = scanFromFile("TableName", EQ_OP, forscanvalue, attributeNames, rmsi, Cata_attrs, CATALOG_NAME+".rec");
	free(forscanvalue);
	if (rc != success) {
		//printf("scanFromFile in createTable error\n");
		return -1;			
	}
	if (rmsi.getNumOfTuples() == 0) {
		//printf("Table %s do not exist.\n", tableName.c_str());
		return -1;
	}

	// update catalog
	RID rid;
	void *data = malloc(100);
	while(rmsi.getNextTuple(rid, data) != RM_EOF) {
		rc = deleteTupleFromFile(CataRecHandle, rid);
		if (rc != success) {
			return -1;
		}
	}
	rmsi.close();
	free(data);

	// delete record & bitmap file
	string recfile = TABLEDIR+"/"+tableName+".rec", mapfile = TABLEDIR+"/"+tableName+".map";
	rc = PFM->DestroyFile(recfile.c_str());
	if (rc != success) {
		printf("DestroyFile in createTable error\n");
		return -1;
	}
	rc = PFM->DestroyFile(mapfile.c_str());
	if (rc != success) {
		printf("DestroyFile in createTable error\n");
		return -1;
	}
	return 0;
}

RC RM::insertTupleToFile(PF_FileHandle &recHandle, string mapFile, const void *data, RID &rid, const vector<Attribute> &attrs) {
	RC rc;
	// get data length
	// short data_len = 0;
	// for (unsigned i = 0; i < attrs.size(); i++) {
	// 	if (attrs[i].type == TypeVarChar) {
	// 		int charlen;
	// 		memcpy(&charlen, (char*)data+data_len, 4);
	// 		if ((unsigned)charlen > attrs[i].length) return -1;
	// 		data_len += (short)charlen;
	// 	}
	// 	data_len += 4;
	// }
	// record length = data length + 2 bytes
	// short rec_len = data_len + 2;


	// !!
	short length;
	void *formateddata = formatData(attrs, data, &length);
	short rec_len = length;


	// 00 for free, 01 for full
	// open mapfile
	FILE *map = fopen(mapFile.c_str(), "r+b");
	if (map == NULL) {
		printf("%d\n", errno);
	}
	char maskadd[4] = {(char)0xC0, (char)0x30, (char)0x0C, (char)0x03}; 
	//char maskor[4] = {0x3F, 0xCF, 0xF3, 0xFC};
	//char maskx[4] = {0x40, 0x10, 0x04, 0x01};
	char twobits[4];
	PageNum page_number;
	unsigned current_byte = 0;
	short offset, nslot, slot_offset, freespace, emptyslot;
	void *page = malloc(PF_PAGE_SIZE);

	bool found = false;
	char c;

	while ((c = fgetc(map)) != EOF) {

		// parse bits
		twobits[0] = (c&maskadd[0]) >> 6;
		twobits[1] = (c&maskadd[1]) >> 4;
		twobits[2] = (c&maskadd[2]) >> 2;
		twobits[3] = (c&maskadd[3]) >> 0;	

		for (unsigned i = 0; i < 4; i++) {
			// find a free page
			if (twobits[i] == 0x01) {

				page_number = current_byte * 4 + i;
				rc = recHandle.ReadPage(page_number, page);
				assert(rc == success);
				memcpy(&offset, page, 2);
				memcpy(&nslot, (char*)page+2, 2);
				freespace = PF_PAGE_SIZE - nslot*4 - offset;
				// if it's really free
				if (freespace >= rec_len + 4) {
					// find the first empty slot
					emptyslot = -1;
					for (short j = 0; j < nslot; j++) {
						memcpy(&slot_offset, (char *)page + PF_PAGE_SIZE - 4 * (j+1), 2);
						if (slot_offset == -1) {
							emptyslot = j;
							break;
						}
					}
					if (emptyslot == -1) {
						// no empty slot, open a new one
						emptyslot = nslot;
						nslot++;
					}
					// check if full after insert, update bitmap
					if (freespace - rec_len - 4 < MIN_FREE_SPACE) {
						twobits[i] = 0x02;
						char byte = (twobits[0] << 6) | (twobits[1] << 4) | (twobits[2] << 2) | (twobits[3] << 0);
						fseek(map, -1, SEEK_CUR);
						fputc(byte, map);
					}
					found = true;
					break;
				}
			// page not exist in bitmap
			} else if (twobits[i] == 0x00) {

				rc = initNewPage(recHandle);
				assert(rc == success);
				// update bitmap
				twobits[i] = 0x01;
				char byte = (twobits[0] << 6) | (twobits[1] << 4) | (twobits[2] << 2) | (twobits[3] << 0);
				fseek(map, -1, SEEK_CUR);
				fputc(byte, map);
				page_number = recHandle.GetNumberOfPages() - 1;
				emptyslot = 0;
				offset = 4;
				nslot = 0;
				freespace = PF_PAGE_SIZE - nslot*4 - offset;
				nslot++;
				if (freespace < rec_len + 4) {
					printf("Record is too big!!\n");
					exit(-1);
				}
				rc = recHandle.ReadPage(page_number, page);
				assert(rc == success);
				// check if full after insert, update bitmap
				if (freespace - rec_len - 4 < MIN_FREE_SPACE) {
					twobits[i] = 0x02;
					char byte = (twobits[0] << 6) | (twobits[1] << 4) | (twobits[2] << 2) | (twobits[3] << 0);
					fseek(map, -1, SEEK_CUR);
					fputc(byte, map);
				}
				found = true;
				break;
			}
		}
		if (found) break;
		current_byte++;
	}


	// if no free pages
	if (!found) {

		rc = initNewPage(recHandle);
		assert(rc == success);
		fputc(0x40, map);
		page_number = (short)recHandle.GetNumberOfPages() - 1;
		emptyslot = 0;
		offset = 4;
		nslot = 0;
		freespace = PF_PAGE_SIZE - nslot*4 - offset;
		nslot++;
		if (freespace < rec_len + 4) {
			printf("Record is too big!!\n");
			exit(-1);
		}
		// check if full, update bitmap
		if (freespace - rec_len - 4 < MIN_FREE_SPACE) {
			char byte = 0x80;
			fseek(map, -1, SEEK_CUR);
			fputc(byte, map);
		}
		rc = recHandle.ReadPage(page_number, page);
		assert(rc == success);
		found = true;
	}

	if (found) {
		// set slot info
		memcpy((char *)page + PF_PAGE_SIZE - 4 * (emptyslot+1), &offset, 2);
		memcpy((char *)page + PF_PAGE_SIZE - 4 * (emptyslot+1) + 2, &rec_len, 2);
		rid.pageNum = (unsigned)page_number;
		rid.slotNum = (unsigned)emptyslot;

		// insert record
		// short sn = 0;
		// memcpy((char*)page+offset, &sn, 2);
		// offset += 2;
		// memcpy((char*)page+offset, data, (int)data_len);
		// offset += data_len;
		// !!!!!!!!!!!!!!!!!!!!!
		memcpy((char*)page+offset, formateddata, (int)rec_len);
		offset += rec_len;

		// update offset & nslot
		memcpy(page, &offset, 2);
		memcpy((char*)page+2, &nslot, 2);

		// write page
		rc = recHandle.WritePage(page_number, page);
		assert(rc == success);

		free(formateddata);
		free(page);
		fclose(map);
		return 0;
	}

	return -1;
}

RC RM::insertTuple(const string tableName, const void *data, RID &rid) {
	RC rc;
	string recfile = TABLEDIR+"/"+tableName+".rec", mapfile = TABLEDIR+"/"+tableName+".map";

	vector<Attribute> attrs;
	//printf("here\n");
	rc = getAttributes(tableName, attrs);
	if (rc != success) {
		//printf("getAttributes in insertTuple error\n");
		return -1;
	}
	//printf("here");
	// for (int i = 0; i < attrs.size(); i++) {
	// 	cout << attrs[i].name << endl;
	// }
	// open record & bitmap file
	PF_FileHandle recHandle, mapHandle;
	rc = PFM->OpenFile(recfile.c_str(), recHandle);
	if (rc != success) {
		//printf("OpenFile in insertTuple error\n");
		return -1;
	}

	// don't open mapHandle
	// rc = PFM->OpenFile(mapfile.c_str(), mapHandle);
	// if (rc != success) {
	// 	printf("OpenFile in insertTuple error\n");
	// 	return -1;
	// }

	// !!
	// short length;
	// void *format = formatData(attrs, data, &length);
	// FILE *test = fopen("test", "wb");
	// fwrite(format, 1, (int)length, test);
	// fclose(test);


	rc = insertTupleToFile(recHandle, mapfile, data, rid, attrs);
	if (rc != success) {
		//printf("insertTupleToFile in insertTuple error\n");
		return -1;		
	}

	// close record & bitmap file
	rc = PFM->CloseFile(recHandle);
	if (rc != success) {
		//printf("CloseFile in insertTuple error\n");
		return -1;
	}

	// ignore this	
	// rc = PFM->CloseFile(mapHandle);
	// if (rc != success) {
	// 	printf("CloseFile in insertTuple error\n");
	// 	return -1;
	// }	
	return 0;
}

RC RM::readTupleFromFile(const vector<Attribute> &attrs, PF_FileHandle &recHandle, const RID &rid, void *data) {
	RC rc;
	void *page = malloc(PF_PAGE_SIZE);
	rc = recHandle.ReadPage(rid.pageNum, page);
	if (rc != success) {
		printf("ReadPage in readTupleFromFile error\n");
		return -1;
	}

	short offset = 0, rec_len = 0;
	memcpy(&offset, (char*)page+(PF_PAGE_SIZE - 4*(rid.slotNum+1)), 2);
	memcpy(&rec_len, (char*)page+(PF_PAGE_SIZE - 4*(rid.slotNum+1) + 2), 2);
	// record has been deleted
	if (offset == -1) {
		return -1;
	}

	// check if it is updated
	short sn;
	memcpy(&sn, (char*)page+(int)offset, 2);
	if (sn != -1) {
		// it's a valid record
		// memcpy(data, (char*)page+(int)offset+2, (int)rec_len-2);
		// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		void *formateddata = malloc(rec_len);
		memcpy(formateddata, (char*)page+(int)offset, rec_len);
		short length;
		vector<int> indexes;
		for (int i = 0; i < (int)attrs.size(); i++) {
			indexes.push_back(i);
		}
		void *originalformat = getOriginalFormat(indexes, attrs, formateddata, &length);
		memcpy(data, originalformat, (int)length);
		free(formateddata);
		free(originalformat);
		free(page);
		return 0;
	} else {
		// it's a updated record
		unsigned page_number;
		unsigned slot_number;
		memcpy(&page_number, (char*)page+(int)offset+2, 4);
		memcpy(&slot_number, (char*)page+(int)offset+2+4, 4);
		RID new_rid;
		new_rid.pageNum = page_number;
		new_rid.slotNum = slot_number;
		free(page);
		// !!!!!!!!!!!!!!!!!!!!!!!!!!!!
		return readTupleFromFile(attrs, recHandle, new_rid, data);
	}
	return 0;
}

RC RM::readTuple(const string tableName, const RID &rid, void *data) {
	RC rc;
	string recfile = TABLEDIR+"/"+tableName+".rec", mapfile = TABLEDIR+"/"+tableName+".map";
	// open recfile
	PF_FileHandle recHandle;
	rc = PFM->OpenFile(recfile.c_str(), recHandle);
	if (rc != success) {
		//printf("OpenFile in readTuple error\n");
		return -1;
	}

	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);

	if (rc != success) {
		//printf("here\n");
		return -1;
	}
	//printf("before from\n");
	RC returnvalue = readTupleFromFile(attrs, recHandle, rid, data);

	// close recfile
	rc = PFM->CloseFile(recHandle);
	if (rc != success) {
		printf("CloseFile in readTuple error\n");
		return -1;
	}

	return returnvalue;
}

RC RM::readAttribute(const string tableName, const RID &rid, const string attributeName, void *data) {
	RC rc;
	//get original tuple data
	void * buffer = malloc(PF_PAGE_SIZE);
	rc = readTuple(tableName, rid, buffer);
	if (rc != success) {
		//printf("readTuple in readAttribute error\n");
		return -1;
	}
	//get attributes
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);

	//format original tuple data
	short formatDataLength;
	void *formatdata = formatData(attrs, buffer, &formatDataLength);

	//figure out the position of attributeName, in attrs
	int position = -1;
	//printf("%d\n", attrs.size());
	for(int i = 0; i < (int)attrs.size(); i++) {
		if(attrs[i].name == attributeName) {
			position = i;
			break;
		}
	}
	if (position == -1) return -1;

	//printf("%d\n", position);
	void * result = getTheIthAttr(position, attrs, formatdata, &formatDataLength);
	memcpy(data, result, (int)formatDataLength);

	free(formatdata);
	free(result);
	free(buffer);
	return 0;
}

RC RM::getAttributes(const string tableName, vector<Attribute> &attrs) {
	// TODO: check catalog to get the latest schema

	RC rc;
	RM_ScanIterator rmsi;
	int tmp = tableName.length();	
	void *forscanvalue = malloc(4+tmp);
	memcpy(forscanvalue, &tmp, 4);
	memcpy((char*)forscanvalue+4, tableName.c_str(), tmp);
	rc = scanFromFile("TableName", EQ_OP, forscanvalue, Cata_attr_names, rmsi, Cata_attrs, CATALOG_NAME+".rec");
	//printf("here\n");
	free(forscanvalue);
	if (rc != success) {
		printf("scanFromFile in getAttributes error\n");
		return -1;		
	}

	RID rid;
	void *data = malloc(PF_PAGE_SIZE);
	Attribute attr;
	//printf("%d\n", rmsi.getNumOfTuples());
	if (rmsi.getNumOfTuples() == 0) return -1;

	//printf("%d\n", rmsi.getNumOfTuples());

	while (rmsi.getNextTuple(rid, data) != RM_EOF) {
		//printf("here\n");
		int len = 0, val = 0, off = 0;
		// skip TableName
		memcpy(&len, (char*)data+off, 4);
		//cout << string((char*)data+off, len) << endl;
		off += (4+len);
		// ColName
		memcpy(&len, (char*)data+off, 4);
		off += 4;
		attr.name = string((char*)data+off, len);
		off += len;
		// Type
		memcpy(&val, (char*)data+off, 4);
		attr.type = (AttrType)val;
		off += 4;
		// Length
		memcpy(&val, (char*)data+off, 4);
		attr.length = val;
		off += 4;
		// skip Schema
		// memcpy(&val, (char*)data+off, 4);
		// skip pagefile name
		attrs.push_back(attr);
	}
	rmsi.close();

	free(data);
	return 0;
}

RC RM::initNewPage(PF_FileHandle &fileHandle) {
	RC rc;

	// init (1) free space offset (2) # of slots
	void *page = malloc(PF_PAGE_SIZE);
	short val = 4;
	memcpy(page, &val, 2);
	val = 0;
	memcpy((char*)page+2, &val, 2);
	
	// append the page
	rc = fileHandle.AppendPage(page);
	if (rc != success) return -1;
	free(page);
	return 0;
}

void *filterRecord(const vector<string> &attributeNames, vector<Attribute> &attrs, char *record, int *length) {
	// calculate length first
	int datalen = 0, len = 0, off = 0, add = 0;
	for (unsigned i = 0; i < attrs.size(); i++) {
		add = 4;
		if (attrs[i].type == TypeVarChar) {
			memcpy(&len, (char*)record+off, 4);
			add += len;
		}
		for (unsigned j = 0; j < attributeNames.size(); j++) {
			if (attributeNames[j] == attrs[i].name) {
				datalen += add;
				break;
			}
		}
		off += add;
	}
	*length = datalen;
	void *data = malloc(datalen);
	// truncate data
	int dataoff = 0;
	len = off = add = 0;
	for (unsigned i = 0; i < attrs.size(); i++) {
		add = 4;
		if (attrs[i].type == TypeVarChar) {
			memcpy(&len, (char*)record+off, 4);
			add += len;
		}
		for (unsigned j = 0; j < attributeNames.size(); j++) {
			if (attributeNames[j] == attrs[i].name) {
				memcpy((char*)data+dataoff, (char*)record+off, add);
				dataoff += add;
				break;
			}
		}
		off += add;
	}
	
	return data;
}

RC RM::scanFromFile(
      const string conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator,
      vector<Attribute> &attrs,
      string recname) {

	// check if conditionAttribute in attrs
	bool validcondition = false;
	for (unsigned i = 0; i < attrs.size(); i++) {
		if (conditionAttribute == attrs[i].name) validcondition = true;
	}

	if (!validcondition) {
		if (compOp == NO_OP) {
			// do nothing
		} else {
			printf("not valid\n");
			return -1;
		}
	}
	RC rc;
	rc = rm_ScanIterator.init(recname, attrs, attributeNames);
	//printf("here\n");
	PF_FileHandle recHandle;
	rc = PFM->OpenFile(recname.c_str(), recHandle);
	if (rc != 0) {
		printf("%d\n", errno);
		printf("OpenFile in scanFromFile error\n");
		return -1;
	}

	void *page = malloc(PF_PAGE_SIZE);
	//printf("here!");
	for (unsigned i = 0; i < recHandle.GetNumberOfPages(); i++) {
		rc = recHandle.ReadPage(i, page);
		if (rc != success) {
			printf("ReadPage in scanFromFile error\n");
			return -1;
		}

		// get number of slots
		short nslot = 0;
		memcpy(&nslot, (char*)page+2, 2);
		for (short j = 0; j < nslot; j++) {
			short offset, length;
			memcpy(&offset, (char*)page+(PF_PAGE_SIZE - j*4 - 4), 2);
			memcpy(&length, (char*)page+(PF_PAGE_SIZE - j*4 - 2), 2);

			// skip deleted slot
			if (offset == -1) continue;

			// skip pointer (updated record)
			short sn;
			memcpy(&sn, (char*)page+offset, 2);		
			if (sn == -1) continue;	

			RID rid;
			rid.pageNum = i;
			rid.slotNum = (unsigned)j;

			if (compOp == NO_OP) {
				rm_ScanIterator.addTuple(rid);
				//printf("%d %d\n", rid.pageNum, rid.slotNum);
				continue;
			}

			void *formateddata = malloc((int)length);
			memcpy(formateddata, (char*)page+offset, (int)length);
			short ori_len;
			void *original;


			
			// find offset in record
			//short off = 2;
			for (unsigned k = 0; k < attrs.size(); k++) {
				if (conditionAttribute == attrs[k].name) {
					bool match = false;
					original = getTheIthAttr(k, attrs, formateddata, &ori_len);
					if (attrs[k].type == TypeVarChar) {
						//printf("here\n");
						int lenval, lenrec;
						memcpy(&lenval, value, 4);
						memcpy(&lenrec, original, 4);
						string quevalue = string((char*)value+4, lenval);							
						string recvalue = string((char*)original+4, lenrec);
						//cout << lenval << " " << lenrec << endl;
						switch(compOp) {
						case EQ_OP:
							if (quevalue == recvalue) match = true;
							break;
						case LT_OP:
							if (quevalue > recvalue) match = true;
							break;
						case GT_OP:
							if (quevalue < recvalue) match = true;
							break;
						case LE_OP:
							if (quevalue >= recvalue) match = true;
							break;
						case GE_OP:
							if (quevalue <= recvalue) match = true;
							break;															
						case NE_OP:
							if (quevalue != recvalue) match = true;
							break;								
						case NO_OP:
								match = true;
							break;					
						}
					} else if (attrs[k].type == TypeInt) {
						int val, rec;
						memcpy(&val, value, 4);
						memcpy(&rec, original, 4);

						switch(compOp) {
						case EQ_OP:
							if (val == rec) match = true;
							break;
						case LT_OP:
							if (val > rec) match = true;
							break;
						case GT_OP:
							if (val < rec) match = true;
							break;
						case LE_OP:
							if (val == rec || val > rec) match = true;
							break;
						case GE_OP:
							if (val == rec || val < rec) match = true;
							break;															
						case NE_OP:
							if (val != rec) match = true;
							break;								
						case NO_OP:
								match = true;
							break;					
						}
					} else if (attrs[k].type == TypeReal) {
						float val, rec, e = 0.001;
						memcpy(&val, value, 4);
						memcpy(&rec, original, 4);

						switch(compOp) {
						case EQ_OP:
							if (abs(val - rec) < e) match = true;
							break;
						case LT_OP:
							if (val > rec) match = true;
							break;
						case GT_OP:
							if (val < rec) match = true;
							break;
						case LE_OP:
							if (abs(val - rec) < e || val > rec) match = true;
							break;
						case GE_OP:
							if (abs(val - rec) < e || val < rec) match = true;
							break;															
						case NE_OP:
							if (val != rec) match = true;
							break;								
						case NO_OP:
								match = true;
							break;					
						}
					}
					free(original);
					if (match) {
						//printf("here\n");
						//original = getOriginalFormat(all_indexes, attrs, formateddata, &ori_len);
						//void *tt = getOriginalFormat(all_indexes, attrs, formateddata, &ori_len);
						//cout << string((char*)original+15, 7) << endl;
						rm_ScanIterator.addTuple(rid);
						//printf("%d\n", ori_len);							
					}
					break;

				}
			}
			
			free(formateddata);
		}
	}
	rc = PFM->CloseFile(recHandle);
	if (rc != 0) {
		printf("CloseFile in scanFromFile error\n");
		return -1;
	}
	free(page);
	return 0;
}

RC RM::scan(const string tableName,
	const string conditionAttribute,
 	const CompOp compOp,                  // comparision type such as "<" and "="
	const void *value,                    // used in the comparison
	const vector<string> &attributeNames, // a list of projected attributes
	RM_ScanIterator &rm_ScanIterator) {
	
	RC rc;
	string recfile = TABLEDIR+"/"+tableName+".rec", mapfile = TABLEDIR+"/"+tableName+".map";

	vector<Attribute> attrs; // attrs got from the catalog
	rc = getAttributes(tableName, attrs);
	if (rc != success) {
		//printf("getAttributes in insertTuple error\n");
		return -1;
	}

	PF_FileHandle recHandle;
	rc = PFM->OpenFile(recfile.c_str(), recHandle);
	if (rc != success) {
		//printf("OpenFile in readTuple error\n");
		return -1;
	}
	//printf("here\n");
	rc = scanFromFile(conditionAttribute, compOp, value, attributeNames, rm_ScanIterator, attrs, recfile);
	//printf("here\n");
	if (rc != success) {
		//printf("scanFromFile in scan error\n");
		return -1;		
	}

	rc = PFM->CloseFile(recHandle);
	if (rc != success) {
		//printf("CloseFile in scan error\n");
		return -1;
	}

	return 0;
}

RC RM_ScanIterator::addTuple(const RID &rid) {
	Rid_list->push_back(rid);
	return 0;
}

RC RM_ScanIterator::init(string recname, const vector<Attribute> &attrs, const vector<string> &attributeNames) {
	RC rc;
	Rid_list = new vector<RID>();
	current_tuple = 0;

	all_indexes = new vector<int>();
	all_attrs = new vector<Attribute>();
	for (int i = 0; i < (int)attrs.size(); i++) {
		all_attrs->push_back(attrs[i]);
		for (int j = 0; j < (int)attributeNames.size(); j++) {
			if (attrs[i].name == attributeNames[j]) {
				all_indexes->push_back(i);
				break;
			}
		}
	}

	PF_Manager *scanpf = PF_Manager::Instance();
	//cout << recname << endl;
	rc = scanpf->OpenFile(recname.c_str(), scanHandle);
	if (rc != success) {
		printf("%d\n", errno);
		printf("init error\n");
		return -1;
	}

	scanbuffer = malloc(PF_PAGE_SIZE);

	return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	RC rc;
	if (current_tuple == Rid_list->size()) return RM_EOF;
	rid = Rid_list->at(current_tuple);
	//void *buffer = malloc(PF_PAGE_SIZE);
	RM* scanrm = RM::Instance();
	//printf("%d\n", all_attrs.size());
	// for (int i = 0; i < all_attrs.size(); i++) {
	// 	cout << all_attrs[i].name << endl;
	// }
	rc = scanrm->readTupleFromFile(*all_attrs, scanHandle, rid, scanbuffer);
	if (rc != success) {
		printf("readTupleFromFile problem!\n");
		return -1;
	}
	//printf("here\n");
	// void *formatData(const vector<Attribute> &attrs, const void *data, short *length)
	short len;
	void *formated = formatData(*all_attrs, scanbuffer, &len);
	fetchRecord(data, *all_indexes, *all_attrs, formated, &len);
	//printf("here\n");
	current_tuple++;
	free(formated);
	//free(buffer);
	return 0;
}

RC RM_ScanIterator::close() {
	RC rc;
	delete Rid_list;
	delete all_indexes;
	delete all_attrs;
	free(scanbuffer);
	PF_Manager *scanpf = PF_Manager::Instance();
	rc = scanpf->CloseFile(scanHandle);
	if (rc != success) {
		return -1;
	}
	return 0;
}

int RM_ScanIterator::getNumOfTuples() {
	return (int)Rid_list->size();
}

RC RM::deleteTupleFromFile(PF_FileHandle &recHandle, const RID &rid) {
	RC rc;
	void *page = malloc(PF_PAGE_SIZE);
	rc = recHandle.ReadPage(rid.pageNum, page);
	if (rc != success) {
		return -1;
	}

	short sn, offset;
	memcpy(&offset, (char*)page + PF_PAGE_SIZE - (rid.slotNum+1) * 4, 2);
	if (offset == -1) {
		return -1;
	}
	memcpy(&sn, (char*)page+offset, 2);
	short nooffset = -1;
	memcpy((char*)page + PF_PAGE_SIZE - (rid.slotNum+1) * 4, &nooffset, 2);

	//write back to file
	rc = recHandle.WritePage(rid.pageNum, page);
	if (rc != success) {
		return -1;
	} 

	if (sn == -1) {
		unsigned page_number;
		unsigned slot_number;
		memcpy(&page_number, (char*)page+offset+2, 4);
		memcpy(&slot_number, (char*)page+offset+2+4, 4);
		RID new_rid;
		new_rid.pageNum = page_number;
		new_rid.slotNum = slot_number;
		free(page);
		return deleteTupleFromFile(recHandle, new_rid);
	}


	free(page);
	return 0;
}


RC RM::deleteTuple(const string tableName, const RID &rid) {
	string recfile = TABLEDIR+"/"+tableName+".rec";
	RC rc;
	PF_FileHandle recHandle;
	rc = PFM->OpenFile(recfile.c_str(), recHandle);
	if (rc != success) {
		return -1;
	}

	rc = deleteTupleFromFile(recHandle, rid);
	if (rc != success) {
		return -1;
	}
	//printf("before close\n");
	rc = PFM->CloseFile(recHandle);
	if (rc != success) {
		return -1;
	}
	return 0;
}

RC RM::deleteTuples(const string tableName) {
	string recfile = TABLEDIR+"/"+tableName+".rec", mapfile = TABLEDIR+"/"+tableName+".map";
	PF_FileHandle recHandle;
	RC rc;
	rc = PFM->OpenFile(recfile.c_str(), recHandle);
	if (rc != success) {
		return -1;
	}
	rc = PFM->CloseFile(recHandle);
	if (rc != success) {
		return -1;
	}
	remove(recfile.c_str());
	remove(mapfile.c_str());


	rc = PFM->CreateFile(recfile.c_str());
	if (rc != success) {
		//printf("CreateFile in createTable error\n");
		return -1;
	}
	rc = PFM->CreateFile(mapfile.c_str());
	if (rc != success) {
		//printf("CreateFile in createTable error\n");
		return -1;
	}

	PF_FileHandle fileHandle;
	rc = PFM->OpenFile(mapfile.c_str(), fileHandle);
	if (rc != success) {
		//printf("OpenFile in createTable error\n");
		return -1;
	}
	void *page = calloc(PF_PAGE_SIZE, 1);
	rc = fileHandle.AppendPage(page);
	free(page);
	if (rc != success) {
		//printf("appendPage in createTable error\n");
		return -1;
	}
	rc = PFM->CloseFile(fileHandle);
	if (rc != success) {
		//printf("CloseFile in createTable error\n");
		return -1;
	}
	return 0;
}

RC RM::reorganizePage(const string tableName, const unsigned pageNumber) {
	string mapFileName, recFileName;
	recFileName = TABLEDIR + "/" + tableName + ".rec";
	mapFileName = TABLEDIR + "/" + tableName + ".map";
	RC rc;
	PF_FileHandle recFileHandle;
	rc = PFM->OpenFile(recFileName.c_str(), recFileHandle);
	if (rc != success) {
		return -1;
	}
	void *previousPage = malloc(PF_PAGE_SIZE);
	void *updatedPage = malloc(PF_PAGE_SIZE);

	rc = recFileHandle.ReadPage(pageNumber, previousPage);
	if (rc != success) {
		return -1;
	}
	bool full = false;
	short slotNum, original_offset;
	memcpy(&original_offset, (char *)previousPage, 2);
	memcpy(&slotNum, (char *)previousPage + 2, 2);
	if (PF_PAGE_SIZE - original_offset - slotNum*4 < MIN_FREE_SPACE) {
		full = true;
	}
	short pageOffset = 4;
	short slotOffset, slotLength;
	for(int i = 0; i < slotNum; i++) {
		memcpy(&slotOffset, (char *)previousPage + PF_PAGE_SIZE  - 4 * (i + 1), 2);
		memcpy(&slotLength, (char *)previousPage + PF_PAGE_SIZE  - 4 * (i + 1) + 2, 2);
		if(slotOffset == -1) {
			//copy slot info
			memcpy((char*)updatedPage + PF_PAGE_SIZE - 4 * (i + 1), (char*)previousPage + PF_PAGE_SIZE - 4 * (i + 1), 4);			
			continue;   // record has been discarded
		}

		if(slotOffset >= 4) { //copy record to the updated page
			//copy slot info
			memcpy((char*)updatedPage + PF_PAGE_SIZE - 4 * (i + 1), &pageOffset, 2);
			memcpy((char*)updatedPage + PF_PAGE_SIZE - 4 * (i + 1) + 2, &slotLength, 2);
			memcpy((char*)updatedPage + pageOffset, (char*)previousPage + slotOffset, (int)slotLength);
			pageOffset += slotLength;
		}
	}
	memcpy(updatedPage, &pageOffset, 2);
	memcpy((char*)updatedPage+2, &slotNum, 2);
	rc = recFileHandle.WritePage(pageNumber, updatedPage);
	if (rc != success) {
		return -1;
	}
	rc = PFM->CloseFile(recFileHandle);
	if (rc != success) {
		return -1;
	}

	// check free space, update bitmap
	if (PF_PAGE_SIZE - pageOffset - slotNum*4 >= MIN_FREE_SPACE && full) {
		unsigned bytenum = pageNumber / 4;
		unsigned bitpos = pageNumber % 4;
		FILE* map = fopen(mapFileName.c_str(), "r+b");
		fseek(map, bytenum, SEEK_SET);
		char byte = fgetc(map);
		char maskadd[4] = {(char)0xC0, (char)0x30, (char)0x0C, (char)0x03};
		char twobits[4];
		twobits[0] = (byte&maskadd[0]) >> 6;
		twobits[1] = (byte&maskadd[1]) >> 4;
		twobits[2] = (byte&maskadd[2]) >> 2;
		twobits[3] = (byte&maskadd[3]) >> 0;
		twobits[bitpos] = 0x01;
		byte = (twobits[0] << 6) | (twobits[1] << 4) | (twobits[2] << 2) | (twobits[3] << 0);
		fseek(map, -1, SEEK_CUR);
		fputc(byte, map);
		fclose(map);
	}

	free(previousPage);
	free(updatedPage);
	return 0;
}

RC RM::updateTuple(const string tableName, const void *data, const RID &rid) {
	string mapFileName, recFileName;
	recFileName = this->TABLEDIR + "/" + tableName + ".rec";
	mapFileName = this->TABLEDIR + "/" + tableName + ".map";

	RC rc;
	PF_FileHandle recFileHandle;
	rc = PFM->OpenFile(recFileName.c_str(), recFileHandle);
	if (rc != success) {
		return -1;
	}

	void *pageData = malloc(PF_PAGE_SIZE);
	rc = recFileHandle.ReadPage(rid.pageNum, pageData);
	if (rc != success) {
		return -1;
	}

	//get the slotOffset, slotLength and schema
	unsigned slotNum = rid.slotNum;
	short slotOffset;
	memcpy((char*)&slotOffset, (char *)pageData + PF_PAGE_SIZE - 4 * (slotNum + 1), 2);
	short slotLength;
	memcpy((char*)&slotLength, (char *)pageData + PF_PAGE_SIZE - 4 * (slotNum + 1) + 2, 2);
	short schema;
	memcpy((char *)&schema, (char *)pageData + slotOffset, 2);

	//get the table Attributes
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
	if (rc != success) {
		cout<<"get Attributes error!"<<endl;
		return -1;
	}

	//get the length of data. NOTICE: the void *data don't include 2 byte schema!!!
	short dataLength = 0;
	for (unsigned i = 0; i < attrs.size(); i++) {
		if (attrs[i].type == TypeVarChar) {
			int charLen;
			memcpy((char *)&charLen, (char *)data + dataLength, 4);
			dataLength += charLen;
		}
		dataLength += 4;
	}

	//change the original data into formated data (with schema = 0)
	short formatedDataLength;
	void * formatedData = formatData(attrs, data, &formatedDataLength);

	//tuple itself is not a pointer
	if(schema != -1) {
		if(formatedDataLength <= slotLength) {
			// update record.
			memcpy((char*)pageData + slotOffset, (char *)formatedData, formatedDataLength);
			//update slot length
			memcpy((char*)pageData + PF_PAGE_SIZE - 4 * (slotNum + 1) + 2, &formatedDataLength, 2);
			recFileHandle.WritePage(rid.pageNum, pageData);
		} else {  //*************
			//create a new tuple to store data(orignial one without schema), current tuple points to the new one
			//update the slotlength (schema)2 + (rid)8 = 10
			short temp = 10;
			memcpy((char*)pageData + PF_PAGE_SIZE - 4 * (slotNum + 1) + 2, &temp, 2);
			//make schema -1 indicates the beginning of a record is a pointer
			temp = -1;
			memcpy((char*)pageData + slotOffset, &temp, 2);
			recFileHandle.WritePage(rid.pageNum, pageData);

			//after write back of slot update, insert the new tuple
			RID new_rid;
			rc = insertTuple(tableName, data, new_rid);
			if (rc != success) {
				return -1;
			}

			recFileHandle.ReadPage(rid.pageNum, pageData);
			//update the record pointer 4 + 4
			memcpy((char*)pageData + slotOffset + 2, &new_rid.pageNum, 4);
			memcpy((char*)pageData + slotOffset + 6, &new_rid.slotNum, 4);
			recFileHandle.WritePage(rid.pageNum, pageData);
		}
	}
	//tuple itself is a pointer
	if(schema == -1) {
		//get the rid of the record to which the pointer points
		RID recordPointer;
		memcpy(&recordPointer.pageNum, (char *)pageData + slotOffset + 2, 4);
		memcpy(&recordPointer.slotNum, (char *)pageData + slotOffset + 6, 4);
		recFileHandle.WritePage(rid.pageNum, pageData);

		//insert a new tuple for data, and let current tuple points to this new one
		RID newRecordRid;
		rc = insertTuple(tableName, data, newRecordRid);
		if (rc != success) {
			return -1;
		}
		recFileHandle.ReadPage(rid.pageNum, pageData);
		memcpy((char *)pageData + slotOffset + 2, &newRecordRid.pageNum, 4);
		memcpy((char *)pageData + slotOffset + 6, &newRecordRid.slotNum, 4);
		recFileHandle.WritePage(rid.pageNum, pageData);
		//delete the record to which pointer points
		rc = deleteTuple(tableName, recordPointer);
		if (rc != success) {
			return -1;
		}
	}
	free(pageData);
	free(formatedData);
	PFM->CloseFile(recFileHandle);
	return 0;
}

RC RM::reorganizeTable(const string tableName) {
	RC rc;
	void * oriPageBuffer = malloc(PF_PAGE_SIZE);

	//get table attributes
	vector<Attribute> attrs;
	rc = this->getAttributes(tableName, attrs);
	if(rc != 0) {
		cout<<"Get Attributes Error in reorganizeTable()"<<endl;
		return -1;
	}

	//open original file
	PF_FileHandle oriRecFileHandle;
	string oriRecFileName = this->TABLEDIR + "/" + tableName + ".rec";
	rc = this->PFM->OpenFile(oriRecFileName.c_str(), oriRecFileHandle);

	//create a temp table for reorganize
	string tempTableName = "temp" + tableName;
	rc = this->createTable(tempTableName, attrs);

	//for every page in original file, copy its tuples into tempTable(Use insertTuple())
	//NOTICE: the tuple may be a pointer, the schema would be -1.
	unsigned i, j;
	RID oriRid, newRid;
	void * tupleBuffer = malloc(PF_PAGE_SIZE);
	for(i = 0; i < (unsigned)oriRecFileHandle.GetNumberOfPages(); i++) {
		oriRecFileHandle.ReadPage(i, oriPageBuffer);
		short pageSlotNum;
		memcpy(&pageSlotNum, (char *)oriPageBuffer + 2, 2);
		for(j = 0; j < (unsigned)pageSlotNum; j++) {
			oriRid.pageNum = i; oriRid.slotNum = j;
			short slotOffset, slotLength;
			memcpy(&slotOffset, (char *)oriPageBuffer + PF_PAGE_SIZE - 4*(j + 1), 2);
			memcpy(&slotLength, (char *)oriPageBuffer + PF_PAGE_SIZE - 4*(j + 1) + 2, 2);

			//if slotOffset == -1, it means this tuple is deleted. skip this one.
			if(slotOffset == -1)
				continue;

			//analyze tuple schema
			short sn;
			memcpy(&sn, (char *)tupleBuffer + slotOffset, 2);

			if(sn != -1) {
				rc = this->readTuple(tableName, oriRid, tupleBuffer);
				if(rc != success) {
					return -1;
				}
				//insert this tuple into temp table
				rc = this->insertTuple(tempTableName, tupleBuffer, newRid);
				if(rc != success) {
					return -1;
				}
			}

			if(sn == -1) {
				unsigned ptPageNum, ptSlotNum;
				memcpy(&ptPageNum, (char *)tupleBuffer + slotOffset + 2, 4);
				memcpy(&ptSlotNum, (char *)tupleBuffer + slotOffset + 6, 4);
				oriRid.pageNum = ptPageNum; oriRid.slotNum = ptSlotNum;

				rc = this->readTuple(tableName, oriRid, tupleBuffer);
				if(rc != success) {
					return -1;
				}

				rc = this->insertTuple(tempTableName, tupleBuffer, newRid);
				if(rc != success) {
					return -1;
				}

				rc = this->deleteTuple(tableName, oriRid);
				if(rc != success) {
					return -1;
				}

			}

		}
	}

	//copy the temp Table data into tableName. Then delete temp table
	this->deleteTuples(tableName);

	string recFileName = this->TABLEDIR + "/" + tableName + ".rec";
	string mapFileName = this->TABLEDIR + "/" + tableName + ".map";
	string tempRecFileName = this->TABLEDIR + "/" + tempTableName + ".rec";
	string tempMapFileName = this->TABLEDIR + "/" + tempTableName + ".map";

	ifstream fi;
	ofstream fo;
	fi.open(tempRecFileName.c_str(), ios::binary);
	fo.open(recFileName.c_str(), ios::binary);
	fo << fi.rdbuf();
	fi.close();
	fo.close();

	fi.open(tempMapFileName.c_str(), ios::binary);
	fo.open(mapFileName.c_str(), ios::binary);
	fo << fi.rdbuf();
	fi.close();
	fo.close();

	this->deleteTable(tempTableName);

	free(oriPageBuffer);
	return 0;
}

