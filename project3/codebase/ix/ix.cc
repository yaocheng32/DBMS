#include "ix.h"

#include <cassert>
#include <cmath>
#include <iostream>
#include <fstream>
// For mkdir
#include <sys/stat.h>
#include <sys/types.h>

IX_Manager *IX_Manager::_ix_manager = 0;

IX_IndexHandle::IX_IndexHandle() {
	Opened = false;
}

IX_IndexHandle::~IX_IndexHandle() {

} 

PF_FileHandle IX_IndexHandle::getFileHandle() const {
	return PageFileHandle;
}

AttrType IX_IndexHandle::getIndexType() const {
	return IndexType;
}

RC IX_IndexHandle::openFileHandle(string filename) {
	RC rc;
	PF_Manager *pfm = PF_Manager::Instance();
	rc = pfm->OpenFile(filename.c_str(), PageFileHandle);
	if (rc == -1) {
		cout << "Creating file " + filename + "...(need to check!!)" << endl; 
		rc = pfm->CreateFile(filename.c_str());
		assert(rc == 0);
	}
	Opened = true;
	return 0;
}

RC IX_IndexHandle::closeFileHandle() {
	RC rc;
	PF_Manager *pfm = PF_Manager::Instance();
	rc = pfm->CloseFile(PageFileHandle);
	if (rc == -1) {
		return -1;
	}
	Opened = false;
	return 0;
}

RC IX_IndexHandle::setIndexType(AttrType type) {
	this->IndexType = type;
	return 0;
}

// returns the total number of slots
short IX_IndexHandle::getSlotNumber(const void *page) const {
	short num;
	memcpy(&num, (char*)page + PF_PAGE_SIZE - 6, 2);
	return num;
}

// returns the free space offset
short IX_IndexHandle::getFreeOffset(const void *page) {
	short off;
	memcpy(&off, (char*)page + PF_PAGE_SIZE - 4, 2);
	return off;
}

// returns the offset closest to the last slot
short IX_IndexHandle::getSlotOffset(const void *page) {
	return PF_PAGE_SIZE - (6 + getSlotNumber(page)*2);
}

// returns total free space in a page
short IX_IndexHandle::getFreeSpace(const void *page) {
	short space = getSlotOffset(page) - getFreeOffset(page);
	return space;
}

// returns the page type
int IX_IndexHandle::getPageType(const void *page) const {
	short t;
	memcpy(&t, (char*)page + PF_PAGE_SIZE - 2, 2);
	return (int)t;
}

// get extra ptr
PageNum IX_IndexHandle::getExtraPtr(const void *page) const {
	PageNum pn;
	memcpy(&pn, page, 4);
	return pn;
}

// get the key tuple of a slot given slot number
RC IX_IndexHandle::getKeyOfSlotInNode(const void *page, short slot_num, void *keyvalue, PageNum *pagenum) const {
	short keyoffset;
	memcpy(&keyoffset, (char*)page + PF_PAGE_SIZE - (6 + (slot_num+1)*2), 2);
	int key_len = 4;
	if (IndexType == TypeVarChar) {
		int tmp;
		memcpy(&tmp, (char*)page + (int)keyoffset, 4);
		key_len = 4+tmp;
		memcpy(keyvalue, (char*)page + (int)keyoffset, key_len);
	} else {
		memcpy(keyvalue, (char*)page + (int)keyoffset, 4);
	}
	memcpy(pagenum, (char*)page + (int)keyoffset + key_len, 4);
	return 0;
}

// get the key tuple of a slot given slot number
RC IX_IndexHandle::getKeyOfSlotInLeaf(const void *page, short slot_num, void *keyvalue, RID &rid) const {
	short keyoffset;
	memcpy(&keyoffset, (char*)page + PF_PAGE_SIZE - (6 + (slot_num+1)*2), 2);
	int key_len = 4;
	if (IndexType == TypeVarChar) {
		int tmp;
		memcpy(&tmp, (char*)page + (int)keyoffset, 4);
		key_len = 4+tmp;
		memcpy(keyvalue, (char*)page + (int)keyoffset, key_len);
	} else {
		memcpy(keyvalue, (char*)page + (int)keyoffset, 4);
	}
	memcpy(&rid, (char*)page + (int)keyoffset + key_len, 8);
	// unsigned tmp;
	// memcpy(&tmp, (char*)page + (int)keyoffset + key_len, 4);
	// printf("%d\n", tmp);
	return 0;
}


RC IX_IndexHandle::findPlaceToInsertInLeaf(const void *page, const void *key, short *slotnum) const {
	if (getPageType(page) != IX_LEAF) {
		printf("This is not a leaf page!\n");
		exit(-1);
	}
	void *buffer = malloc(PF_PAGE_SIZE);
	RID tmprid;
	int keylen, valuelen;
	bool keep, same, found = false;
	int num_dup = 0;
	short last_found_slot = -1;
	for (short i = getSlotNumber(page) - 1; i >= 0; i--) {
		getKeyOfSlotInLeaf(page, i, buffer, tmprid);
		if (IndexType == TypeVarChar) {
			memcpy(&keylen, key, 4);
			memcpy(&valuelen, buffer, 4);
			string keystring = string((char*)key+4, keylen);
			string valuestring = string((char*)buffer+4, valuelen);
			keep = (keystring >= valuestring);
			same = (keystring == valuestring);
		} else if (IndexType == TypeInt) {
			int keyint, valueint;
			memcpy(&keyint, key, 4);
			memcpy(&valueint, buffer, 4);
			keep = (keyint >= valueint);
			same = (keyint == valueint);
		} else if (IndexType == TypeReal) {
			float keyfloat, valuefloat;
			memcpy(&keyfloat, key, 4);
			memcpy(&valuefloat, buffer, 4);
			keep = (abs(keyfloat - valuefloat) <= 0.001) || (keyfloat > valuefloat);
			same = (abs(keyfloat - valuefloat) <= 0.001);
		}

		if (same) {
			last_found_slot = i; // !!
			num_dup++;
		}

		if (keep && !found) {
			last_found_slot = i;
			found = true;
		}

		if (!keep && found) {
			break;
		}
	}
	*slotnum = last_found_slot;
	int tmp;
	memcpy(&tmp, key, 4);
	//printf("insert %d, find %d\n", tmp, last_found_slot);
	return num_dup;
}

// get the RID with key in a node, and get the corresponding slot number
RID IX_IndexHandle::getKeyRidInLeaf(const void *page, const void *key, short *slotnum, int *num_dup) const {
	if (getPageType(page) != IX_LEAF) {
		printf("This is not a leaf page!\n");
		exit(-1);
	}	
	void *buffer = malloc(PF_PAGE_SIZE);
	RID rid;
	int keylen, valuelen;
	bool found = false;
	int how_many = 0;
	short last_found_slot = -1;
	for (short i = getSlotNumber(page) - 1; i >= 0; i--) {
		getKeyOfSlotInLeaf(page, i, buffer, rid);
		if (IndexType == TypeVarChar) {
			memcpy(&keylen, key, 4);
			memcpy(&valuelen, buffer, 4);
			string keystring = string((char*)key+4, keylen);
			string valuestring = string((char*)buffer+4, valuelen);
			found = (keystring == valuestring);
		} else if (IndexType == TypeInt) {
			int keyint, valueint;
			memcpy(&keyint, key, 4);
			memcpy(&valueint, buffer, 4);
			found = (keyint == valueint);
		} else if (IndexType == TypeReal) {
			float keyfloat, valuefloat;
			memcpy(&keyfloat, key, 4);
			memcpy(&valuefloat, buffer, 4);
			found = (abs(keyfloat - valuefloat) <= 0.001);
		}
		if (found) {
			how_many++;
			last_found_slot = i;
			//printf("%d\n", rid.pageNum);
		}
	}
	
	*slotnum = last_found_slot;
	*num_dup = how_many;
	//printf("%d\n", last_found_slot);
	// should not use found!!
	if (last_found_slot != -1) {
		getKeyOfSlotInLeaf(page, last_found_slot, buffer, rid);
		//printf("here\n");
	} else {
		*num_dup = -1;
	}
	free(buffer);
	return rid;
}

// get the page pointer with key in a node, and get the corresponding slot number
PageNum IX_IndexHandle::getKeyPtrInNode(const void *page, const void *key, short *slotnum) const {
	if (getPageType(page) != IX_NODE) {
		printf("This is not a node page!\n");
		exit(-1);
	}

	void *buffer = malloc(PF_PAGE_SIZE);
	PageNum pagenum;
	int keylen, valuelen;
	bool found;
	for (short i = getSlotNumber(page) - 1; i >= 0; i--) {
		getKeyOfSlotInNode(page, i, buffer, &pagenum);
		found = false;
		if (IndexType == TypeVarChar) {
			memcpy(&keylen, key, 4);
			memcpy(&valuelen, buffer, 4);
			string keystring = string((char*)key+4, keylen);
			string valuestring = string((char*)buffer+4, valuelen);
			found = (keystring >= valuestring);
		} else if (IndexType == TypeInt) {
			int keyint, valueint;
			memcpy(&keyint, key, 4);
			memcpy(&valueint, buffer, 4);
			found = (keyint >= valueint);
		} else if (IndexType == TypeReal) {
			float keyfloat, valuefloat;
			memcpy(&keyfloat, key, 4);
			memcpy(&valuefloat, buffer, 4);
			found = (keyfloat > valuefloat) || (abs(keyfloat - valuefloat) <= 0.001);
		}
		if (found) {
			free(buffer);
			*slotnum = i;
			return pagenum;
		}
	}
	free(buffer);
	*slotnum = -1;
	return getExtraPtr(page);
}


// set the free offset
RC IX_IndexHandle::setFreeOffset(void *page, short offset) {
	if (offset >= PF_PAGE_SIZE - 6) {
		return -1;
	}
	memcpy((char*)page + PF_PAGE_SIZE - 4, &offset, 2);
	return 0;
}

// set the slot offset
RC IX_IndexHandle::setSlotNumber(void *page, short num) {
	if (num < 0) {
		return -1;
	}
	memcpy((char*)page + PF_PAGE_SIZE - 6, &num, 2);
	return 0;
}

// set the page type
RC IX_IndexHandle::setPageType(void *page, int type) {
	short t = (short)type;
	memcpy((char*)page + PF_PAGE_SIZE - 2, &t, 2);
	return 0;
}

// set extra ptr
RC IX_IndexHandle::setExtraPtr(void *page, PageNum pagenum) {
	memcpy(page, &pagenum, 4);
	return 0;
}

// sorted version: complete
// add key to a node
RC IX_IndexHandle::addKeyToNode(void *page, const void *key, PageNum pagenum) {
	if (getPageType(page) != IX_NODE) {
		printf("This is not a node page!\n");
		return -1;
	}

	int data_len = 8;
	if (IndexType == TypeVarChar) {
		int tmp;
		memcpy(&tmp, key, 4);
		data_len += tmp;
	}

	// if (data_len + 2/*for slot*/ > getFreeSpace(page)) {
	// 	return -1;
	// }

	// find place to insert
	PageNum tmppn;
	short sn;
	tmppn = getKeyPtrInNode(page, key, &sn);
	short cur_free_off = getFreeOffset(page),
				cur_slot_off = getSlotOffset(page),
				cur_slot_num = getSlotNumber(page);

	// move rest slots
	short num_to_move = cur_slot_num - sn - 1;
	void *tmpbuf = malloc((int)num_to_move*2);
	memcpy(tmpbuf, (char*)page+(int)cur_slot_off, (int)num_to_move*2);
	memcpy((char*)page+(int)cur_slot_off-2, tmpbuf, (int)num_to_move*2);
	free(tmpbuf);
	// insert new slot
	memcpy((char*)page+(int)cur_slot_off-2+num_to_move*2, &cur_free_off, 2);
	// insert key & pagenum
	memcpy((char*)page+(int)cur_free_off, key, data_len - 4);
	memcpy((char*)page+(int)cur_free_off+data_len - 4, &pagenum, 4);
	// update info
	cur_free_off += (short)data_len;
	cur_slot_num++;
	setFreeOffset(page, cur_free_off);
	setSlotNumber(page, cur_slot_num);

	return 0;
}

// add key to a leaf
RC IX_IndexHandle::addKeyToLeaf(void *page, const void *key, const RID &rid) {
	if (getPageType(page) != IX_LEAF) {
		printf("This is not a leaf page!\n");
		return -1;
	}

	int data_len = 12;
	if (IndexType == TypeVarChar) {
		int tmp;
		memcpy(&tmp, key, 4);
		data_len += tmp;
	}

	// find place to insert
	short sn;
	int num_dup = findPlaceToInsertInLeaf(page, key, &sn);
	short cur_free_off = getFreeOffset(page),
				cur_slot_off = getSlotOffset(page),
				cur_slot_num = getSlotNumber(page);

	// move rest slots
	short num_to_move = cur_slot_num - sn - 1;
	void *tmpbuf = malloc((int)num_to_move*2);
	memcpy(tmpbuf, (char*)page+(int)cur_slot_off, (int)num_to_move*2);
	memcpy((char*)page+(int)cur_slot_off-2, tmpbuf, (int)num_to_move*2);
	free(tmpbuf);
	// insert new slot
	memcpy((char*)page+(int)cur_slot_off-2+num_to_move*2, &cur_free_off, 2);
	// insert key & rid
	memcpy((char*)page+(int)cur_free_off, key, data_len - 8);
	memcpy((char*)page+(int)cur_free_off + data_len - 8, &rid, 8);
	// unsigned tmp;
	// memcpy(&tmp, (char*)page+(int)cur_free_off+data_len, 4);
	// printf("%d\n", tmp);
	// update info
	cur_free_off += (short)data_len;
	cur_slot_num++;
	setFreeOffset(page, cur_free_off);
	setSlotNumber(page, cur_slot_num);

	return num_dup;
}

// initialize the page with type (0 for node, 1 for leaf)
RC IX_IndexHandle::initPage(void *page, int type) {
	setExtraPtr(page, 0); // need to check if null?
	setFreeOffset(page, 4);
	setSlotNumber(page, 0);
	setPageType(page, type);
	return 0;
}


bool IX_IndexHandle::shouldSplit(const void *page, const void *key) {
	if (IndexType == TypeVarChar) {
		int len;
		memcpy(&len, key, 4);
		if (getFreeSpace(page) < len + IX_FREE_SPACE) return true;
	} else {
		if (2*IX_ORDER == getSlotNumber(page)) return true;
	}
	return false;
}

RC IX_IndexHandle::getPageOfKey(void *key, PageNum target, PageNum *pn, short *sn) const {
	RC rc;
	void *page = malloc(PF_PAGE_SIZE);
	rc = PageFileHandle.ReadPage(target, page);
	assert(rc == 0);
	int type = getPageType(page);
	if (type == IX_NODE) {
		PageNum next = getKeyPtrInNode(page, key, sn);
		free(page);
		if (next == 0) {
			return -1;
		}
		return getPageOfKey(key, next, pn, sn);
	} else if (type == IX_LEAF) {
		//int tmpd;
		//getKeyRidInLeaf(page, key, sn, &tmpd);
		findPlaceToInsertInLeaf(page, key, sn);
		free(page);
		*pn = target;
		//if (tmpd == -1) return -1;
		return 0;
	}
	return 0;
}

RC IX_IndexHandle::getLeftmost(void *key, PageNum target, PageNum *pn) const {
	RC rc;
	void *page = malloc(PF_PAGE_SIZE);
	rc = PageFileHandle.ReadPage(target, page);
	assert(rc == 0);
	int type = getPageType(page);	
	if (type == IX_NODE) {
		PageNum next = getExtraPtr(page);
		free(page);
		if (next == 0) {
			return -1;
		}
		return getLeftmost(key, next, pn);
	} else if (type == IX_LEAF) {
		free(page);
		*pn = target;
		return 0;
	}
	return 0;
}


RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle, CompOp compOp, void *value) {
	RC rc;
	
	if (!indexHandle.isOpened()) {
		return -1;
	}

	// init queue
	ridqueue = new deque<RID>();

	// find starting page
	// =, >, >=: search and scan
	// <, <=, !=: scan from left
	PageNum start_page;
	short start_slot = 0;
	if (compOp == EQ_OP || compOp == GT_OP || compOp == GE_OP) {
		//printf("here\n");
		rc = indexHandle.getPageOfKey(value, IX_ROOT_PAGE_NUMBER, &start_page, &start_slot);
		if (rc == -1) {
			return 0;
		}
	} else {
		rc = indexHandle.getLeftmost(value, IX_ROOT_PAGE_NUMBER, &start_page);
		if (rc == -1) {
			return 0;
		}
	}
	// start scan
	void *page = malloc(PF_PAGE_SIZE);
	rc = indexHandle.getFileHandle().ReadPage(start_page, page);
	assert(rc == 0);
	void *buffer = malloc(PF_PAGE_SIZE);
	if (start_slot == -1) start_slot = 0;
	// start_slot = 0; // should use this !!
	//printf("%d\n", start_page);
	while(1) {
		if (IX_LEAF != indexHandle.getPageType(page)) {
			printf("OpenScan scaning a node!\n");
			return -1;
		}

		bool over = false, match;
		RID rid;
		//short tmpsn;
		//int num_dup;
		//if (start_slot == -1) break; // no match
		
		for (short i = start_slot; i < indexHandle.getSlotNumber(page); i++) {
			indexHandle.getKeyOfSlotInLeaf(page, i, buffer, rid);
			match = false;
			switch(compOp) {
				case EQ_OP: // = value
					if (indexHandle.getIndexType() == TypeInt) {
						int valueint, foundint;
						memcpy(&valueint, value, 4);
						memcpy(&foundint, buffer, 4);
						if (valueint == foundint) match = true;
						if (foundint < valueint) over = true;
					} else if (indexHandle.getIndexType() == TypeReal) {
						float valuefloat, foundfloat;
						memcpy(&valuefloat, value, 4);
						memcpy(&foundfloat, buffer, 4);
						if (abs(valuefloat - foundfloat) < 0.001) match = true;
						if (foundfloat < valuefloat) over = true;
					} else if (indexHandle.getIndexType() == TypeVarChar) {
						int valuelen, foundlen;
						memcpy(&valuelen, value, 4);
						memcpy(&foundlen, buffer, 4);
						string valuestr = string((char*)value+4, valuelen);
						string foundstr = string((char*)buffer+4, foundlen);
						if (valuestr == foundstr) match = true;
						if (foundstr < valuestr) over = true;
					}
					break;
				case GT_OP: // > value
					if (indexHandle.getIndexType() == TypeInt) {
						int valueint, foundint;
						memcpy(&valueint, value, 4);
						memcpy(&foundint, buffer, 4);
						if (foundint > valueint) match = true;
					} else if (indexHandle.getIndexType() == TypeReal) {
						float valuefloat, foundfloat;
						memcpy(&valuefloat, value, 4);
						memcpy(&foundfloat, buffer, 4);
						if (foundfloat > valuefloat) match = true;
					} else if (indexHandle.getIndexType() == TypeVarChar) {
						int valuelen, foundlen;
						memcpy(&valuelen, value, 4);
						memcpy(&foundlen, buffer, 4);
						string valuestr = string((char*)value+4, valuelen);
						string foundstr = string((char*)buffer+4, foundlen);
						if (foundstr > valuestr) match = true;
					}
					break;
				case GE_OP: // >= value
					if (indexHandle.getIndexType() == TypeInt) {
						int valueint, foundint;
						memcpy(&valueint, value, 4);
						memcpy(&foundint, buffer, 4);
						if (foundint >= valueint) match = true;
					} else if (indexHandle.getIndexType() == TypeReal) {
						float valuefloat, foundfloat;
						memcpy(&valuefloat, value, 4);
						memcpy(&foundfloat, buffer, 4);
						if ((abs(valuefloat - foundfloat) < 0.001) || (foundfloat > valuefloat)) match = true;
					} else if (indexHandle.getIndexType() == TypeVarChar) {
						int valuelen, foundlen;
						memcpy(&valuelen, value, 4);
						memcpy(&foundlen, buffer, 4);
						string valuestr = string((char*)value+4, valuelen);
						string foundstr = string((char*)buffer+4, foundlen);
						if (foundstr >= valuestr) match = true;
					}
					break;
				case LT_OP: // < value
					if (indexHandle.getIndexType() == TypeInt) {
						int valueint, foundint;
						memcpy(&valueint, value, 4);
						memcpy(&foundint, buffer, 4);
						if (foundint < valueint) match = true;
					} else if (indexHandle.getIndexType() == TypeReal) {
						float valuefloat, foundfloat;
						memcpy(&valuefloat, value, 4);
						memcpy(&foundfloat, buffer, 4);
						if (foundfloat < valuefloat) match = true;
					} else if (indexHandle.getIndexType() == TypeVarChar) {
						int valuelen, foundlen;
						memcpy(&valuelen, value, 4);
						memcpy(&foundlen, buffer, 4);
						string valuestr = string((char*)value+4, valuelen);
						string foundstr = string((char*)buffer+4, foundlen);
						if (foundstr < valuestr) match = true;
					}
					if (!match) over = true;
					break;
				case LE_OP: // <= value
					if (indexHandle.getIndexType() == TypeInt) {
						int valueint, foundint;
						memcpy(&valueint, value, 4);
						memcpy(&foundint, buffer, 4);
						if (foundint <= valueint) match = true;
					} else if (indexHandle.getIndexType() == TypeReal) {
						float valuefloat, foundfloat;
						memcpy(&valuefloat, value, 4);
						memcpy(&foundfloat, buffer, 4);
						if ((abs(valuefloat - foundfloat) < 0.001) || (foundfloat < valuefloat)) match = true;
					} else if (indexHandle.getIndexType() == TypeVarChar) {
						int valuelen, foundlen;
						memcpy(&valuelen, value, 4);
						memcpy(&foundlen, buffer, 4);
						string valuestr = string((char*)value+4, valuelen);
						string foundstr = string((char*)buffer+4, foundlen);
						if (foundstr <= valuestr) match = true;
					}
					if (!match) over = true;
					break;
				case NE_OP: // != value
					if (indexHandle.getIndexType() == TypeInt) {
						int valueint, foundint;
						memcpy(&valueint, value, 4);
						memcpy(&foundint, buffer, 4);
						//printf("value: %d %d\n", valueint, foundint);
						if (foundint != valueint) match = true;
					} else if (indexHandle.getIndexType() == TypeReal) {
						float valuefloat, foundfloat;
						memcpy(&valuefloat, value, 4);
						memcpy(&foundfloat, buffer, 4);
						if (abs(valuefloat - foundfloat) >= 0.001) match = true;
					} else if (indexHandle.getIndexType() == TypeVarChar) {
						int valuelen, foundlen;
						memcpy(&valuelen, value, 4);
						memcpy(&foundlen, buffer, 4);
						string valuestr = string((char*)value+4, valuelen);
						string foundstr = string((char*)buffer+4, foundlen);
						if (foundstr != valuestr) match = true;
					}
					break;
				default:
					match = true;
					break;
			}
			
			if (over) break;

			if (match) {
				ridqueue->push_back(rid);
			}
			//if (match == false) break; // should check!
		}

		if (over) break;

		PageNum next_page = indexHandle.getExtraPtr(page);
		if (next_page == 0) break;
		rc = indexHandle.getFileHandle().ReadPage(next_page, page);
		assert(rc == 0);
		start_slot = 0;
	}
	free(buffer);
	free(page);
	return 0;
}

RC IX_IndexScan::GetNextEntry(RID &rid) {
	if (!ridqueue->empty()) {
		rid = ridqueue->at(0);
		ridqueue->pop_front();
		return 0;
	} else {
		return IX_EOF;
	}
}

RC IX_IndexScan::CloseScan() {
	delete ridqueue;
	return 0;
}

RC IX_IndexHandle::InsertEntry(void *key, const RID &rid) {
	RC rc;
	if (!Opened) {
		printf("Page file handle not open\n");
		return -1;
	}
	void *newkey = malloc(PF_PAGE_SIZE);
	PageNum newpage;
	rc = InsertEntryToNode(0, key, rid, newkey, newpage);
	free(newkey);
	return rc;
}

// 1 if 1 > 2, 0 if =, -1 if 1 < 2
int IX_IndexHandle::compareKey(void *key1, void *key2) {
	if (IndexType == TypeVarChar) {
		int len1, len2;
		memcpy((void*)&len1, key1, 4);
		memcpy((void*)&len2, key2, 4);
		string str1 = string((char*)key1+4, len1);
		string str2 = string((char*)key2+4, len2);
		if (str1 > str2) return 1;
		if (str1 == str2) return 0;
		return -1;
	} else if (IndexType == TypeInt) {
		int val1, val2;
		memcpy((void*)&val1, key1, 4);
		memcpy((void*)&val2, key2, 4);
		if (val1 > val2) return 1;
		if (val1 == val2) return 0;
		return -1;
	} else {
		float val1, val2;
		memcpy((void*)&val1, key1, 4);
		memcpy((void*)&val2, key2, 4);
		if (val1 > val2) return 1;
		if (abs(val1-val2) < 0.001) return 0;
		return -1;
	}
	return 0;
}


RC IX_IndexHandle::splitLeaf(PageNum original, void *key, const RID &rid, PageNum &new_append, void *middlekey) {
	RC rc;
	void *page = malloc(PF_PAGE_SIZE);
	rc = PageFileHandle.ReadPage(original, page);
	if (rc == -1) {
		printf("Read page error\n");
		return -1;
	}

	bool insertToLeft = true;

	void *newleftpage = malloc(PF_PAGE_SIZE);
	initPage(newleftpage, IX_LEAF);
	void *newrightpage = malloc(PF_PAGE_SIZE);
	initPage(newrightpage, IX_LEAF);
	setExtraPtr(newrightpage, getExtraPtr(page));

	bool found = false;
	RID slotrid;
	void *slotkey = malloc(PF_PAGE_SIZE);
	for (short i = getSlotNumber(page) - 1 ; i >= 0; i--) {
		rc = getKeyOfSlotInLeaf(page, i, slotkey, slotrid);
		if (rc == -1) {
			printf("getKeyOfSlotInLeaf error\n");
			return -1;
		}
		if (compareKey(key, slotkey) >= 0 && !found) {
			if (IndexType == TypeVarChar) {
				if (i >= getSlotNumber(page)/2) {
					insertToLeft = false;
				}
				found = true;
			} else {
				if ((int)i >= IX_ORDER) {
					insertToLeft = false;
				}
				found = true;
			}
		}
		
		// copy to new page
		if (IndexType == TypeVarChar) {
			if (i >= getSlotNumber(page)/2) {
				addKeyToLeaf(newrightpage, slotkey, slotrid);
			} else {
				addKeyToLeaf(newleftpage, slotkey, slotrid);
			}
		} else {
			if ((int)i >= IX_ORDER) {
				addKeyToLeaf(newrightpage, slotkey, slotrid);
			} else {
				addKeyToLeaf(newleftpage, slotkey, slotrid);
			}
		}
	}
	free(slotkey);

	if (insertToLeft) {
		addKeyToLeaf(newleftpage, key, rid);
	} else {
		addKeyToLeaf(newrightpage, key, rid);
	}

	getKeyOfSlotInLeaf(newrightpage, 0, middlekey, slotrid);
	PageFileHandle.AppendPage(newrightpage);
	new_append = PageFileHandle.GetNumberOfPages() - 1;
	setExtraPtr(newleftpage, new_append);
	// remeber to write back
	PageFileHandle.WritePage(original, newleftpage);
	free(newleftpage);
	free(newrightpage);
	free(page);
	return 0;
}


RC IX_IndexHandle::splitNode(PageNum original, void *key, PageNum pointer, PageNum &new_append, void *middlekey) {
	RC rc;
	void *page = malloc(PF_PAGE_SIZE);
	rc = PageFileHandle.ReadPage(original, page);
	if (rc == -1) {
		printf("Read page error\n");
		return -1;
	}

	bool insertToLeft = true;

	void *newleftpage = malloc(PF_PAGE_SIZE);
	initPage(newleftpage, IX_NODE);
	setExtraPtr(newleftpage, getExtraPtr(page));	
	void *newrightpage = malloc(PF_PAGE_SIZE);
	initPage(newrightpage, IX_NODE);

	bool found = false;
	PageNum slotptr;
	void *slotkey = malloc(PF_PAGE_SIZE);
	for (short i = getSlotNumber(page) - 1 ; i >= 0; i--) {
		rc = getKeyOfSlotInNode(page, i, slotkey, &slotptr);
		if (rc == -1) {
			printf("getKeyOfSlotInNode error\n");
			return -1;
		}
		if (compareKey(key, slotkey) >= 0 && !found) {
			if (IndexType == TypeVarChar) {
				if (i >= getSlotNumber(page)/2) {
					insertToLeft = false;
				}
				found = true;
			} else {
				if ((int)i >= IX_ORDER) {
					insertToLeft = false;
				}
				found = true;
			}
		}
		
		// copy to new page
		if (IndexType == TypeVarChar) {
			if (i >= getSlotNumber(page)/2) {
				addKeyToNode(newrightpage, slotkey, slotptr);
			} else {
				addKeyToNode(newleftpage, slotkey, slotptr);
			}
		} else {
			if ((int)i >= IX_ORDER) {
				addKeyToNode(newrightpage, slotkey, slotptr);
			} else {
				addKeyToNode(newleftpage, slotkey, slotptr);
			}
		}
	}
	free(slotkey);

	if (insertToLeft) {
		addKeyToNode(newleftpage, key, pointer);
	} else {
		addKeyToNode(newrightpage, key, pointer);
	}

	getKeyOfSlotInNode(newrightpage, 0, middlekey, &slotptr);
	setExtraPtr(newrightpage, slotptr);
	deleteSlotInNode(newrightpage, 0);
	PageFileHandle.AppendPage(newrightpage);
	new_append = PageFileHandle.GetNumberOfPages() - 1;
	//setExtraPtr(newleftpage, new_append);
	// remeber to write back
	PageFileHandle.WritePage(original, newleftpage);
	free(newleftpage);
	free(newrightpage);
	free(page);
	return 0;
}

RC IX_IndexHandle::createNewRoot(void *newkey, PageNum newpage) {
	void *newrootpage = malloc(PF_PAGE_SIZE);
	initPage(newrootpage, IX_NODE);
	void *originalrootpage = malloc(PF_PAGE_SIZE);
	PageFileHandle.ReadPage(0, originalrootpage);
	// set extra ptr
	PageFileHandle.AppendPage(originalrootpage); // new left ptr
	setExtraPtr(newrootpage, PageFileHandle.GetNumberOfPages() - 1);

	//void *rightpage = malloc(PF_PAGE_SIZE);
	//PageFileHandle.ReadPage(newpage, rightpage);
	//void *key = malloc(PF_PAGE_SIZE);
	//PageNum tmppn;
	//getKeyOfSlotInNode(rightpage, 0, key, &tmppn);
	addKeyToNode(newrootpage, newkey, newpage);

	PageFileHandle.WritePage(0, newrootpage);
	free(originalrootpage);
	//free(rightpage);
	free(newrootpage);
	//free(key);
	return 0;
}

// note: return > 0 value to indicate duplicate
RC IX_IndexHandle::InsertEntryToNode(PageNum current, void *key, const RID &rid, void *newkey, PageNum &newpage) {
	RC rc;
	void *page = malloc(PF_PAGE_SIZE);
	rc = PageFileHandle.ReadPage(current, page);
	if (rc == -1) {
		printf("Read page error\n");
		return -1;
	}
	if (getPageType(page) == IX_NODE) {

		// node empty
		if (getExtraPtr(page) == 0) {
			void *newleaf = malloc(PF_PAGE_SIZE);
			initPage(newleaf, IX_LEAF);
			PageFileHandle.AppendPage(newleaf);
			setExtraPtr(page, PageFileHandle.GetNumberOfPages() - 1);
			PageFileHandle.WritePage(current, page);
			free(newleaf);
		}

		PageFileHandle.ReadPage(current, page);

		short tmpsn;
		PageNum child = getKeyPtrInNode(page, key, &tmpsn);
		//printf("%d\n", child);
		//free(page);
		RC returnvalue = InsertEntryToNode(child, key, rid, newkey, newpage);
		
		if (returnvalue == -1) {
			return -1;
		}

		if (newpage == 0) {
			return returnvalue;
		}

		rc = PageFileHandle.ReadPage(current, page);
		if (rc == -1) {
			printf("Read page error\n");
			return -1;
		}

		if (shouldSplit(page, newkey)) {
			// split
			free(page);
			splitNode(current, newkey, newpage, newpage, newkey);
			// check if root
			if (current == 0) {
				createNewRoot(newkey, newpage);
			}

			
			return returnvalue;
		} else {
			rc = addKeyToNode(page, newkey, newpage);
			if (rc == -1) {
				printf("addKeyToNode error\n");
				return -1;
			}

			rc = PageFileHandle.WritePage(current, page);
			if (rc == -1) {
				printf("Write page error\n");
				return -1;
			}

			newpage = 0;

			free(page);
			return 0;
		}

	} else {
		if (getSlotNumber(page) > 0) {
			short tmpsn;
			int tmpdup;
			RID ridfound = getKeyRidInLeaf(page, key, &tmpsn, &tmpdup);
			if (rid.pageNum == ridfound.pageNum && rid.slotNum == ridfound.slotNum) {
				return -1;
			}
		}
		if (shouldSplit(page, key)) {
			// split
			free(page);
			splitLeaf(current, key, rid, newpage, newkey);			
			return 1;
		} else {
			RC returnvalue = addKeyToLeaf(page, key, rid);
			if (returnvalue == -1) {
				printf("addKeyToLeaf error\n");
				return -1;
			}

			rc = PageFileHandle.WritePage(current, page);
			if (rc == -1) {
				printf("Write page error\n");
				return -1;
			}

			newpage = 0;
			free(page);
			return 0;
		}
	}
}

IX_Manager *IX_Manager::Instance() {
	if(!_ix_manager)
		_ix_manager = new IX_Manager();

	return _ix_manager;
}


IX_Manager::IX_Manager() {
	this->INDEXDIR = "Indexes";
	mkdir(INDEXDIR.c_str(), 0777);

	this->thisPFM = PF_Manager::Instance();
	this->thisRM = RM::Instance();
}

RC IX_Manager::CreateIndex(const string tableName,
		const string attributeName) {
	RC rc;
	string ixName = INDEXDIR + "/" + tableName + "_" + attributeName + ".ix";

	//create a file
	rc = thisPFM->CreateFile(ixName.c_str());
	if(rc == -1) {
		cout<<"Index file has already existed!"<<endl;
		return -1;
	}

	//create an IX_IndexHandle instance, insert data
	IX_IndexHandle indexHandle;
	rc = this->OpenIndex(tableName, attributeName, indexHandle);
	assert(rc == 0);


	//first init the root page, and let it points to an empty leaf node whose pageNum = 1
	indexHandle.initRootNode();

	//read all records from data file by using RecordManager,then insert the records one by one
	RM_ScanIterator scanIter;
	vector<string> attributeNames;
	attributeNames.push_back(attributeName);
	void * data = calloc(PF_PAGE_SIZE, 1);
	thisRM->scan(tableName, "", NO_OP, NULL, attributeNames, scanIter);

	RID rid;

	if(scanIter.getNumOfTuples() > 0) {
		int i = 0;
		while(scanIter.getNextTuple(rid, data) != RM_EOF) {
			if(i == 100 || i == 99) {
				int c;
				c = 0;
			}
			//RC duplicate = indexHandle.InsertEntry(data, rid);
			indexHandle.InsertEntry(data, rid);
			i++;
		}

	}
	scanIter.close();
	indexHandle.closeFileHandle();
	free(data);
	return 0;
}

RC IX_Manager::DestroyIndex(const string tableName,
		const string attributeName) {
	RC rc;
	string indexName = INDEXDIR + "/" + tableName + "_" + attributeName + ".ix";

	//if index doesn't exist, return -1
	fstream f;
	f.open(indexName.c_str(), ios::in);
	if(!f) {
		cout<<"Index file doesn't exist!"<<endl;
		return -1;
	}
	f.close();

	//else, delete the index file
	rc = thisPFM->DestroyFile(indexName.c_str());
	return rc;
}

RC IX_Manager::OpenIndex(const string tableName,
		const string attributeName,
		IX_IndexHandle &indexHandle) {

	string indexName = INDEXDIR + "/" + tableName + "_" + attributeName + ".ix";

	//if index doesn't exist, return -1
	fstream f;
	f.open(indexName.c_str(), ios::in);
	if(!f) {
		cout<<"Index file doesn't exist!"<<endl;
		return -1;
	}
	f.close();

	//set PageFileHandle & Opened in indexHandle
	indexHandle.openFileHandle(indexName);

	//set IndexType
	vector<Attribute> attrs;
	this->thisRM->getAttributes(tableName, attrs);
	for(unsigned i = 0; i < attrs.size(); i++) {
		if(attrs[i].name == attributeName) {
			indexHandle.setIndexType(attrs[i].type);
			break;
		}
	}

	return 0;
}

RC IX_Manager::CloseIndex(IX_IndexHandle &indexHandle) {
	RC rc;
	rc = indexHandle.closeFileHandle();
	return rc;
}

RC IX_IndexHandle::initRootNode() {
	if(!this->Opened) {
		cout<<"This IX_IndexHandle has not opened an index file!"<<endl;
		return -1;
	}

	//to use this function, make sure the index file is total empty.
	if(this->PageFileHandle.GetNumberOfPages() > 0) {
		cout<<"Root existed! Error!"<<endl;
		return -1;
	}

	//append a root node. and let the root node points to an empty leaf node whose pageNum = 1
	void * data = calloc(PF_PAGE_SIZE, 1);
	setExtraPtr(data, 0);
	setPageType(data, IX_NODE);
	setSlotNumber(data, 0);
	setFreeOffset(data, 4);
	PageFileHandle.AppendPage(data);

	// setExtraPtr(data, 0);
	// setPageType(data, IX_LEAF);
	// setSlotNumber(data, 0);
	// setFreeOffset(data, 4);
	// PageFileHandle.AppendPage(data);

	free(data);
	return 0;

}

// new 11/16
RC IX_IndexHandle::deleteSlotInLeaf(void *page, short slotnum) {
	if (getPageType(page) != IX_LEAF) {
		printf("This is not a leaf!!\n");
		return -1;
	}

	//RC rc;
	// check if it is a valid slotnum
	short total_slots = getSlotNumber(page);
	if (slotnum < 0 || slotnum >= total_slots) {
		printf("wrong slot number\n");
		return -1;
	}

	// remember offset in advance
	short move_to;
	memcpy(&move_to, (char*)page + PF_PAGE_SIZE - 6 - (int)(slotnum+1)*2, 2);

	short tuple_len;
	if (IndexType == TypeVarChar) {
		int keylen;
		memcpy(&keylen, (char*)page+(int)move_to, 4);
		tuple_len = 4 + (short)keylen + 8;
	} else {
		tuple_len = 4 + 8;
	}

	// move rest slots advanced
	void *rest = malloc(PF_PAGE_SIZE);
	int len = (int)(total_slots - slotnum - 1)*2;
	memcpy(rest, (char*)page + PF_PAGE_SIZE - 6 - (int)total_slots*2, len);
	memcpy((char*)page + PF_PAGE_SIZE - 6 - (int)total_slots*2 + 2, rest, len);
	setSlotNumber(page, total_slots-1);

	// move rest tuples advance
	memcpy(rest, (char*)page+(int)move_to+(int)tuple_len, (int)(getFreeOffset(page)-move_to-tuple_len));
	memcpy((char*)page+(int)move_to, rest, (int)(getFreeOffset(page)-move_to-tuple_len));
	setFreeOffset(page, getFreeOffset(page)-tuple_len);

	// update slot offset
	for (short i = 0; i < getSlotNumber(page); i++) {
		short offset;
		memcpy(&offset, (char*)page + PF_PAGE_SIZE - 6 - (int)(i+1)*2, 2);
		if (offset > move_to) {
			offset -= tuple_len;
			memcpy((char*)page + PF_PAGE_SIZE - 6 - (int)(i+1)*2, (void*)&offset, 2);
		}
	}

	free(rest);
	return 0;
}


RC IX_IndexHandle::deleteSlotInNode(void *page, short slotnum) {
	if (getPageType(page) != IX_NODE) {
		printf("This is not a node!!\n");
		return -1;
	}

	//RC rc;
	// check if it is a valid slotnum
	short total_slots = getSlotNumber(page);
	if (slotnum < 0 || slotnum >= total_slots) {
		printf("wrong slot number\n");
		return -1;
	}

	// remember offset in advance
	short move_to;
	memcpy(&move_to, (char*)page + PF_PAGE_SIZE - 6 - (int)(slotnum+1)*2, 2);

	short tuple_len;
	if (IndexType == TypeVarChar) {
		int keylen;
		memcpy(&keylen, (char*)page+(int)move_to, 4);
		tuple_len = 4 + (short)keylen + 4;
	} else {
		tuple_len = 4 + 4;
	}

	// move rest slots advanced
	void *rest = malloc(PF_PAGE_SIZE);
	int len = (int)(total_slots - slotnum - 1)*2;
	memcpy(rest, (char*)page + PF_PAGE_SIZE - 6 - (int)total_slots*2, len);
	memcpy((char*)page + PF_PAGE_SIZE - 6 - (int)total_slots*2 + 2, rest, len);
	setSlotNumber(page, total_slots-1);

	// move rest tuples advance
	memcpy(rest, (char*)page+(int)move_to+(int)tuple_len, (int)(getFreeOffset(page)-move_to-tuple_len));
	memcpy((char*)page+(int)move_to, rest, (int)(getFreeOffset(page)-move_to-tuple_len));
	setFreeOffset(page, getFreeOffset(page)-tuple_len);

	// update slot offset
	for (short i = 0; i < getSlotNumber(page); i++) {
		short offset;
		memcpy(&offset, (char*)page + PF_PAGE_SIZE - 6 - (int)(i+1)*2, 2);
		if (offset > move_to) {
			offset -= tuple_len;
			memcpy((char*)page + PF_PAGE_SIZE - 6 - (int)(i+1)*2, (void*)&offset, 2);
		}
	}

	free(rest);
	return 0;
}


RC IX_IndexHandle::DeleteEntry(void *key, const RID &rid) {
	void *key_to_remove = malloc(PF_PAGE_SIZE);
	PageNum page_to_remove = 0;
	RC rc = DeleteEntryFromNode(0, 0, 0, key, rid, key_to_remove, page_to_remove);
	free(key_to_remove);
	return rc;
}

short IX_IndexHandle::findKeyPairInLeaf(void *page, void *key, const RID &rid) {
	void *buffer = malloc(PF_PAGE_SIZE);
	RID ridfind;
	int keylen, valuelen;
	bool found = false;
	for (short i = getSlotNumber(page) - 1; i >= 0; i--) {
		getKeyOfSlotInLeaf(page, i, buffer, ridfind);
		if (IndexType == TypeVarChar) {
			memcpy(&keylen, key, 4);
			memcpy(&valuelen, buffer, 4);
			string keystring = string((char*)key+4, keylen);
			string valuestring = string((char*)buffer+4, valuelen);
			found = (keystring == valuestring) && ridfind.pageNum == rid.pageNum && ridfind.slotNum == rid.slotNum;
		} else if (IndexType == TypeInt) {
			int keyint, valueint;
			memcpy(&keyint, key, 4);
			memcpy(&valueint, buffer, 4);
			found = (keyint == valueint) && ridfind.pageNum == rid.pageNum && ridfind.slotNum == rid.slotNum;
		} else if (IndexType == TypeReal) {
			float keyfloat, valuefloat;
			memcpy(&keyfloat, key, 4);
			memcpy(&valuefloat, buffer, 4);
			found = (abs(keyfloat - valuefloat) <= 0.001) && ridfind.pageNum == rid.pageNum && ridfind.slotNum == rid.slotNum;
		}
		if (found) {
			free(buffer);
			return i;
		}
	}
	free(buffer);
	return -1;
}
RC IX_IndexHandle::deleteKeyPairInNode(void *page, PageNum page_to_remove) {
	void *tmpkey = malloc(PF_PAGE_SIZE);
	PageNum page_found;

	// rearrange key
	if (getExtraPtr(page) == page_to_remove && getSlotNumber(page) > 0) {
		getKeyOfSlotInNode(page, 0, tmpkey, &page_found);
		deleteSlotInNode(page, 0);
		setExtraPtr(page, page_found);
		free(tmpkey);
		return 0;
	}
	// added

	for (short i = 0; i < getSlotNumber(page); i++) {
		getKeyOfSlotInNode(page, i, tmpkey, &page_found);
		if (page_found == page_to_remove) {
			deleteSlotInNode(page, i);
			free(tmpkey);
			return 0;
		}
	}
	free(tmpkey);
	if (getExtraPtr(page) == page_to_remove) {
		setExtraPtr(page, 0);
		return 0;
	} else {
		printf("deleteKeyPairInNode: not found!!\n");
	}
	return -1;
}

RC IX_IndexHandle::DeleteEntryFromNode(PageNum parent, PageNum current, PageNum leftnode, void *key, const RID &rid, void *key_to_remove, PageNum &page_to_remove) {
	RC rc;
	void *page = malloc(PF_PAGE_SIZE);
	rc = PageFileHandle.ReadPage(current, page);
	if (rc != 0) {
		printf("Read page error\n");
		return -1;
	}

	if (getPageType(page) == IX_NODE) {
		short sn;
		// find child
		PageNum child = getKeyPtrInNode(page, key, &sn);

		if (current == 0 && child == 0) {
			return -1;
		}
		//printf("%d\n", child);
		// find nextleft
		PageNum nextleft;
		void *tmpkey = malloc(PF_PAGE_SIZE);
		if (sn == -1) {
			if (leftnode == 0) { // no leftnode
				nextleft = 0;
			} else { // leftnode exists, find greatest ptr in leftnode
				void *tmppage = malloc(PF_PAGE_SIZE);
				rc = PageFileHandle.ReadPage(leftnode, tmppage);
				if (rc != 0) {
					printf("Read page error\n");
					return -1;
				}
				short left_slot_num = getSlotNumber(tmppage);
				if (left_slot_num == 0) { // leftnode is empty
					nextleft = 0;
				} else {
					getKeyOfSlotInNode(tmppage, getSlotNumber(tmppage)-1, tmpkey, &nextleft);
				}
				free(tmppage);
			}
		} else if (sn == 0) {
			nextleft = getExtraPtr(page);
		} else { // no need to check leftnode
			getKeyOfSlotInNode(page, sn-1, tmpkey, &nextleft);
		}
		free(tmpkey);
		
		
		// recursively find next level
		rc = DeleteEntryFromNode(current, child, nextleft, key, rid, key_to_remove, page_to_remove);

		if (rc == -1) {
			printf("This key is not in the index!\n");
			return -1;
		}

		// no need to remove
		if (page_to_remove == 0) return 0;

		rc = PageFileHandle.ReadPage(current, page);
		if (rc != 0) {
			printf("Read page error\n");
			return -1;
		}

		// remove entry
		deleteKeyPairInNode(page, page_to_remove);

		rc = PageFileHandle.WritePage(current, page);
		if (rc == -1) {
			printf("Write page error\n");
			return -1;
		}

		// examine if node is not empty
		if (getSlotNumber(page) > 0 || getExtraPtr(page) != 0) {
			free(page);
			page_to_remove = 0;
			return 0;
		}

		page_to_remove = current;

		setExtraPtr(page, 0);
		rc = PageFileHandle.WritePage(current, page);
		if (rc != 0) {
			printf("Write page error\n");
			return -1;
		}

		free(page);
		return 0;

	} else { // find leaf
		// check if the key is in the leaf
		short sn = findKeyPairInLeaf(page, key, rid);
		if (sn == -1) { // key pair not in the leaf
			free(page);
			return -1;
		} else {
			rc = deleteSlotInLeaf(page, sn);
			if (rc == -1) {
				printf("deleteSlotInLeaf returns -1\n");
				return -1;
			}
			rc = PageFileHandle.WritePage(current, page);
			if (rc == -1) {
				printf("Write page error\n");
				return -1;
			}
		}

		rc = PageFileHandle.ReadPage(current, page);
		if (rc != 0) {
			printf("Read page error");
			return -1;
		}

		// leaf is not empty after delete
		if (getSlotNumber(page) > 0) {
			free(page);
			page_to_remove = 0;
			return 0;
		}

		// leaf is empty, delete leaf
		// NOTE: what about the empty page?
		page_to_remove = current;
		
		// rearrange right ptr
		if (leftnode != 0) {
			// get right ptr
			PageNum right = getExtraPtr(page);
			// read left page
			void *leftpage = malloc(PF_PAGE_SIZE);
			rc = PageFileHandle.ReadPage(leftnode, leftpage);
			if (rc == -1) {
				printf("Read page error\n");
				return -1;
			}
			setExtraPtr(leftpage, right);
			// write left page
			rc = PageFileHandle.WritePage(leftnode, leftpage);
			if (rc == -1) {
				printf("Read page error\n");
				return -1;
			}
			free(leftpage);
		}

		// ?
		// clean extra ptr
		setExtraPtr(page, 0);
		rc = PageFileHandle.WritePage(current, page);
		if (rc != 0) {
			printf("Write page error\n");
			return -1;
		}

		free(page);
		return 0;
	}
	return 0;
}



// old insert

// /*
//  * InsertEntry Needs another recursive function InsertEntryIntoNode();
//  * void *key size of 4 bytes for int and float, 4 + character num for VarChar
//  */
// RC IX_IndexHandle::InsertEntry(void * key, const RID & rid) {
// //	//First of all, check if this IndexHandle has opened an index
// //	if(!this->isOpen()) {
// //		//cout<<"Error, this IndexHandle has not opened an index yet!"<<endl;
// //		return -1;
// //	}

// 	void * newChildPointer = NULL;
// 	//get the root node
// 	void * rootNode = malloc(PF_PAGE_SIZE);
// 	this->PageFileHandle.ReadPage(0, rootNode);

// 	RC returnValue;
// 	returnValue = InsertEntryIntoNode(rootNode, 0, key, rid, newChildPointer);

// 	free(rootNode);
// 	// 0 means single entry, 1 means duplicates
// 	return returnValue;
// }

// /*
//  * node points to a memory that contains the node info
//  * newChildPointer refers to (void * key + PageNum pageNum)
//  * newChildPointer null initially, and null on return unless child is split
//  */
// RC IX_IndexHandle::InsertEntryIntoNode(void * node, PageNum nodeNum,
// 		void * key, const RID & rid, void * newChildPointer) {
// 	RC rc, returnValue;
// 	//if *nodePointer is a non-leaf node
// 	if(this->getPageType(node) == IX_NODE) {
// 		//choose subtree

// 		PageNum nextNodeNum;
// 		short slotNum;
// 		nextNodeNum = this->getKeyPtrInNode(node, key, &slotNum);

// 		void * nextNode = malloc(PF_PAGE_SIZE);
// 		rc = this->PageFileHandle.ReadPage(nextNodeNum, nextNode);

// 		returnValue = InsertEntryIntoNode(nextNode, nextNodeNum, key, rid, newChildPointer);

// 		if(newChildPointer == NULL) return returnValue;

// 		//newChildPointer is not NULL
// 		//Check if this node has space for newChildEntry
// 		if( !this->shouldSplit(node, newChildPointer)) { //don't need to split
// 			//parse newChildPointer
// 			void * newChildEntryValue = calloc(PF_PAGE_SIZE, 1);
// 			PageNum newChildEntryPage;
// 			if(this->IndexType == TypeInt || IndexType == TypeReal) {
// 				memcpy(newChildEntryValue, newChildPointer, 4);
// 				memcpy(&newChildEntryPage, (char*)newChildPointer + 4, 4);
// 			}
// 			if(this->IndexType == TypeVarChar) {
// 				//TODO:varchar....
// 			}
// 			//insert this new childEntry and write back
// 			this->addKeyToNode(node, newChildEntryValue, newChildEntryPage);
// 			this->PageFileHandle.WritePage(nodeNum, node);
// 			free(newChildPointer);
// 			newChildPointer = NULL;
// 			free(newChildEntryValue);
// 			return returnValue;
// 		}

// 		//this non-leaf node should be split
// 		switch(this->IndexType) {
// 		case TypeInt:
// 			this->splitRegularNode<int>(node, nodeNum, key, rid, newChildPointer);
// 			break;
// 		case TypeReal:
// 			this->splitRegularNode<float>(node, nodeNum, key, rid, newChildPointer);
// 			break;
// 		case TypeVarChar:

// 			break;
// 		}
// 		return returnValue;
// 	}

// 	//if *nodePointer is a leaf node, check available space
// 	//TODO: return a non-zero value if this entry is already in the index
// 	if(this->getPageType(node) == IX_LEAF) {
// 		//have free space. Don't need to split
// 		if( !this->shouldSplit(node, key)) {
// 			//TODO:Need to tell whether duplicate exists
// 			this->addKeyToLeaf(node, key, rid);
// 			this->PageFileHandle.WritePage(nodeNum, node);
// 			free(newChildPointer);
// 			newChildPointer = NULL;
// 			return 0;
// 		}

// 		//don't have free space, need to split
// 		/*
// 		 * insert new entry into this node,node = 2d + 1;
// 		 * then split it to d and d+1
// 		 */
// 		switch(this->IndexType) {
// 		case TypeInt:
// 			returnValue = splitRegularLeaf<int>(node, nodeNum, key, rid, newChildPointer);
// 			break;
// 		case TypeReal:
// 			returnValue = splitRegularLeaf<float>(node, nodeNum, key, rid, newChildPointer);
// 			break;
// 		case TypeVarChar:  //TODO:

// 			break;
// 		}
// 		return returnValue;
// 	}
// }






// tests

// RC prepareStr(void *buf, string str) {
// 	int len = str.length();
// 	memcpy(buf, &len, 4);
// 	memcpy((char*)buf+4, str.c_str(), len);
// 	return 0;
// }

// RC IX_IndexHandle::testNodeStr() {
// 	RC rc;
// 	RC success = 0;
// 	char *testfilename = "testfile";
// 	remove(testfilename);
// 	this->IndexType = TypeVarChar;
// 	PF_Manager *pfm = PF_Manager::Instance();
// 	rc = pfm->CreateFile(testfilename);
// 	assert(rc == success);
// 	rc = pfm->OpenFile(testfilename, this->PageFileHandle);
// 	assert(rc == success);
// 	void *page = malloc(PF_PAGE_SIZE);
// 	rc = initPage(page, IX_NODE);
// 	assert(rc == success);
// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);	

// 	int num_slots = 0;
// 	int cur_free = 4;

// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));


// 	// insert (Cool, 25)
// 	void *key = malloc(PF_PAGE_SIZE);

// 	string str = "Cool";
// 	prepareStr(key, str);
// 	PageNum pn = 25;
// 	rc = addKeyToNode(page, key, pn);
// 	assert(rc == success);
// 	num_slots++;
// 	cur_free+=(4+4+4);

// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));

// 	void *tmpkey = malloc(PF_PAGE_SIZE);
// 	PageNum tmppn;
// 	getKeyOfSlotInNode(page, 0, tmpkey, &tmppn);
// 	assert(0 == memcmp(key, tmpkey, 8));
// 	assert(tmppn == pn);


// 	// insert (Angela, 30)
// 	str = "Angela";
// 	prepareStr(key, str);	
// 	pn = 30;
// 	rc = addKeyToNode(page, key, pn);
// 	assert(rc == success);
// 	num_slots++;
// 	cur_free+=(4+6+4);
// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));

// 	// insert (Bob, 12)
// 	str = "Bob";
// 	prepareStr(key, str);	
// 	pn = 12;
// 	rc = addKeyToNode(page, key, pn);
// 	assert(rc == success);
// 	num_slots++;
// 	cur_free+=(4+3+4);
// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));

// 	// insert (Eason, 82)
// 	str = "Eason";
// 	prepareStr(key, str);	
// 	pn = 82;
// 	rc = addKeyToNode(page, key, pn);
// 	assert(rc == success);
// 	num_slots++;
// 	cur_free+=(4+5+4);
// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));

// 	setExtraPtr(page, 1000);

// 	short sn;
// 	str = "Angela";
// 	prepareStr(key, str);	
// 	PageNum ptr = getKeyPtrInNode(page, key, &sn);
// 	assert(ptr == 30);
// 	assert(sn == 0);
// 	str = "Bob";
// 	prepareStr(key, str);	
// 	ptr = getKeyPtrInNode(page, key, &sn);
// 	assert(ptr == 12);
// 	assert(sn == 1);
// 	str = "Cool";
// 	prepareStr(key, str);	
// 	ptr = getKeyPtrInNode(page, key, &sn);
// 	assert(ptr == 25);	
// 	assert(sn == 2);
// 	str = "Eason";
// 	prepareStr(key, str);	
// 	ptr = getKeyPtrInNode(page, key, &sn);
// 	assert(ptr == 82);
// 	assert(sn == 3);

// 	str = "Angel";
// 	prepareStr(key, str);	
// 	ptr = getKeyPtrInNode(page, key, &sn);
// 	assert(ptr == 1000);

// 	str = "Angelb";
// 	prepareStr(key, str);	
// 	ptr = getKeyPtrInNode(page, key, &sn);
// 	assert(ptr == 30);

// 	str = "Deron";
// 	prepareStr(key, str);	
// 	ptr = getKeyPtrInNode(page, key, &sn);
// 	assert(ptr == 25);

// 	str = "Kobe";
// 	prepareStr(key, str);	
// 	ptr = getKeyPtrInNode(page, key, &sn);
// 	assert(ptr == 82);

// 	// insert duplicate
// 	// insert(Cool, 66)
// 	str = "Cool";
// 	prepareStr(key, str);
// 	pn = 66;
// 	rc = addKeyToNode(page, key, pn);
// 	assert(rc == success);
// 	num_slots++;
// 	cur_free+=(4+4+4);
// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));

// 	str = "Cool";
// 	prepareStr(key, str);
// 	pn = 88;
// 	rc = addKeyToNode(page, key, pn);
// 	assert(rc == success);
// 	num_slots++;
// 	cur_free+=(4+4+4);
// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));
	
// 	getKeyOfSlotInNode(page, 3, tmpkey, &tmppn);
// 	assert(0 == memcmp(key, tmpkey, 8));
// 	assert(tmppn == 66);

// 	getKeyOfSlotInNode(page, 4, tmpkey, &tmppn);
// 	assert(0 == memcmp(key, tmpkey, 8));
// 	assert(tmppn == 88);

// 	free(key);
// 	free(tmpkey);
// 	free(page);

// 	rc = pfm->CloseFile(this->PageFileHandle);
// 	assert(rc == success);
// 	return 0;
// }

// RC IX_IndexHandle::testNode() {
// 	RC rc;
// 	RC success = 0;
// 	char *testfilename = "testfile";
// 	remove(testfilename);
// 	this->IndexType = TypeInt;
// 	PF_Manager *pfm = PF_Manager::Instance();
// 	rc = pfm->CreateFile(testfilename);
// 	assert(rc == success);
// 	rc = pfm->OpenFile(testfilename, this->PageFileHandle);
// 	assert(rc == success);
// 	void *page = malloc(PF_PAGE_SIZE);
// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	rc = PageFileHandle.ReadPage(0, page);
// 	assert(rc == success);

// 	rc = initPage(page, IX_NODE);
// 	assert(rc == success);

// 	int num_slots = 0;

// 	assert(0 == getSlotNumber(page));
// 	assert(4 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 == getFreeSpace(page));

// 	// insert (5, 25)
// 	int key = 5;
// 	PageNum pn = 25;
// 	rc = addKeyToNode(page, (void*)&key, pn);
// 	assert(rc == success);
// 	num_slots++;

// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*8 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*10 == getFreeSpace(page));

// 	int tmpkey;
// 	PageNum tmppn;
// 	getKeyOfSlotInNode(page, 0, (void*)&tmpkey, &tmppn);
// 	assert(tmpkey == key);
// 	assert(tmppn == pn);


// 	// insert (3, 10)
// 	key = 3;
// 	pn = 10;
// 	rc = addKeyToNode(page, (void*)&key, pn);
// 	assert(rc == success);
// 	num_slots++;

// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*8 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*10 == getFreeSpace(page));

// 	// insert (100, 30)
// 	key = 100;
// 	pn = 30;
// 	rc = addKeyToNode(page, (void*)&key, pn);
// 	assert(rc == success);
// 	num_slots++;

// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*8 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*10 == getFreeSpace(page));

// 	// insert (75, 22)
// 	key = 75;
// 	pn = 22;
// 	rc = addKeyToNode(page, (void*)&key, pn);
// 	assert(rc == success);
// 	num_slots++;

// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*8 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*10 == getFreeSpace(page));


// 	short sn;
// 	key = 4;	
// 	PageNum ptr = getKeyPtrInNode(page, &key, &sn);
// 	assert(ptr == 10);
// 	key = 10;
// 	ptr = getKeyPtrInNode(page, &key, &sn);
// 	assert(ptr == 25);
// 	key = 1;
// 	ptr = getKeyPtrInNode(page, &key, &sn);
// 	assert(ptr == 0);
// 	key = 100;
// 	ptr = getKeyPtrInNode(page, &key, &sn);
// 	assert(ptr == 30);	
// 	key = 90;
// 	ptr = getKeyPtrInNode(page, &key, &sn);
// 	assert(ptr == 22);	

// 	// insert duplicate
// 	// insert (75, 40)
// 	key = 75;
// 	pn = 40;
// 	rc = addKeyToNode(page, (void*)&key, pn);
// 	assert(rc == success);
// 	num_slots++;

// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*8 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*10 == getFreeSpace(page));

// 	key = 80;
// 	ptr = getKeyPtrInNode(page, &key, &sn);
// 	assert(ptr == 40);	

// 	rc = PageFileHandle.WritePage(0, page);
// 	assert(rc == success);
// 	free(page);

// 	rc = pfm->CloseFile(this->PageFileHandle);
// 	assert(rc == success);

// 	return 0;
// }

// RC IX_IndexHandle::testLeafStr() {
// 	RC rc;
// 	RC success = 0;
// 	char *testfilename = "testfile";
// 	remove(testfilename);
// 	this->IndexType = TypeVarChar;
// 	PF_Manager *pfm = PF_Manager::Instance();
// 	rc = pfm->CreateFile(testfilename);
// 	assert(rc == success);
// 	rc = pfm->OpenFile(testfilename, this->PageFileHandle);
// 	assert(rc == success);
// 	void *page = malloc(PF_PAGE_SIZE);
// 	rc = initPage(page, IX_LEAF);
// 	assert(rc == success);
// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);	

// 	int num_slots = 0;
// 	int cur_free = 4;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));


// 	// insert (Cool, (10, 20))
// 	void *key = malloc(PF_PAGE_SIZE);

// 	string str = "Cool";
// 	prepareStr(key, str);
// 	RID rid;
// 	rid.pageNum = 10;
// 	rid.slotNum = 20;
// 	rc = addKeyToLeaf(page, key, rid);
// 	assert(rc == success);
// 	num_slots++;
// 	cur_free+=(4+4+8);
// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));

// 	// insert (Alex, (15, 10))
// 	str = "Alex";
// 	prepareStr(key, str);
// 	rid.pageNum = 15;
// 	rid.slotNum = 10;
// 	rc = addKeyToLeaf(page, key, rid);
// 	assert(rc == success);
// 	num_slots++;
// 	cur_free+=(4+4+8);
// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));

// 	// insert (Bob, (151, 10))
// 	str = "Bob";
// 	prepareStr(key, str);
// 	rid.pageNum = 151;
// 	rid.slotNum = 10;
// 	rc = addKeyToLeaf(page, key, rid);
// 	assert(rc == success);
// 	num_slots++;
// 	cur_free+=(4+3+8);
// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));


// 	// insert (ZooKeeper, (11, 19))
// 	str = "ZooKeeper";
// 	prepareStr(key, str);
// 	rid.pageNum = 11;
// 	rid.slotNum = 19;
// 	rc = addKeyToLeaf(page, key, rid);
// 	assert(rc == success);
// 	num_slots++;
// 	cur_free+=(4+9+8);
// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));

// 	short sn;
// 	int tmpd;
// 	str = "Alex";
// 	prepareStr(key, str);	
// 	rid = getKeyRidInLeaf(page, key, &sn, &tmpd);
// 	assert(rid.pageNum == 15);
// 	assert(rid.slotNum == 10);
// 	assert(sn == 0);
// 	str = "Bob";
// 	prepareStr(key, str);	
// 	rid = getKeyRidInLeaf(page, key, &sn, &tmpd);
// 	assert(rid.pageNum == 151);
// 	assert(rid.slotNum == 10);
// 	assert(sn == 1);
// 	str = "Cool";
// 	prepareStr(key, str);	
// 	rid = getKeyRidInLeaf(page, key, &sn, &tmpd);
// 	assert(rid.pageNum == 10);
// 	assert(rid.slotNum == 20);
// 	assert(sn == 2);
// 	str = "ZooKeeper";
// 	prepareStr(key, str);	
// 	rid = getKeyRidInLeaf(page, key, &sn, &tmpd);
// 	assert(rid.pageNum == 11);
// 	assert(rid.slotNum == 19);
// 	assert(sn == 3);

// 	// insert duplicate
// 	// insert (ZooKeeper, (111, 191))
// 	str = "ZooKeeper";
// 	prepareStr(key, str);
// 	rid.pageNum = 111;
// 	rid.slotNum = 191;
// 	rc = addKeyToLeaf(page, key, rid);
// 	assert(rc == 1);
// 	num_slots++;
// 	cur_free+=(4+9+8);
// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));

// 	// insert (ZooKeeper, (1111, 1911))
// 	str = "ZooKeeper";
// 	prepareStr(key, str);
// 	rid.pageNum = 1111;
// 	rid.slotNum = 1911;
// 	rc = addKeyToLeaf(page, key, rid);
// 	assert(rc == 2);
// 	num_slots++;
// 	cur_free+=(4+9+8);
// 	assert(num_slots == getSlotNumber(page));
// 	assert(cur_free == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 - cur_free == getFreeSpace(page));

// 	void *tmpkey = malloc(PF_PAGE_SIZE);
// 	getKeyOfSlotInLeaf(page, 4, tmpkey, rid);
// 	assert(0 == memcmp(key, tmpkey, 13));
// 	assert(rid.pageNum == 1111);
// 	assert(rid.slotNum == 1911);

// 	getKeyOfSlotInLeaf(page, 5, tmpkey, rid);
// 	assert(0 == memcmp(key, tmpkey, 13));
// 	assert(rid.pageNum == 111);
// 	assert(rid.slotNum == 191);

// 	free(key);
// 	free(tmpkey);
// 	free(page);
// 	rc = pfm->CloseFile(this->PageFileHandle);
// 	assert(rc == success);
// 	return 0;
// }

// RC IX_IndexHandle::testLeaf() {
// 	RC rc;
// 	RC success = 0;
// 	char *testfilename = "testfile";
// 	remove(testfilename);
// 	this->IndexType = TypeInt;
// 	PF_Manager *pfm = PF_Manager::Instance();
// 	rc = pfm->CreateFile(testfilename);
// 	assert(rc == success);
// 	rc = pfm->OpenFile(testfilename, this->PageFileHandle);
// 	assert(rc == success);
// 	void *page = malloc(PF_PAGE_SIZE);
// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	rc = PageFileHandle.ReadPage(0, page);
// 	assert(rc == success);

// 	rc = initPage(page, IX_LEAF);
// 	assert(rc == success);

// 	int num_slots = 0;

// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));

// 	// insert (5, (1, 2))
// 	int key = 5;
// 	RID rid;
// 	rid.pageNum = 1;
// 	rid.slotNum = 2;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);
// 	num_slots++;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));

// 	int tmpkey;
// 	RID tmprid;
// 	getKeyOfSlotInLeaf(page, 0, (void*)&tmpkey, tmprid);
// 	assert(tmpkey == key);
// 	assert(tmprid.pageNum == rid.pageNum);
// 	assert(tmprid.slotNum == rid.slotNum);


// 	// insert (3, (2, 3))
// 	key = 3;
// 	rid.pageNum = 2;
// 	rid.slotNum = 3;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);
// 	num_slots++;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));

// 	// insert (100, (5, 10))
// 	key = 100;
// 	rid.pageNum = 5;
// 	rid.slotNum = 10;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);
// 	num_slots++;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));

// 	// insert (75, (20, 15))
// 	key = 75;
// 	rid.pageNum = 20;
// 	rid.slotNum = 15;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);
// 	num_slots++;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));


// 	getKeyOfSlotInLeaf(page, 0, (void*)&tmpkey, tmprid);

// 	assert(tmpkey == 3);
// 	assert(tmprid.pageNum == 2);
// 	assert(tmprid.slotNum == 3);
// 	getKeyOfSlotInLeaf(page, 1, (void*)&tmpkey, tmprid);
// 	assert(tmpkey == 5);
// 	assert(tmprid.pageNum == 1);
// 	assert(tmprid.slotNum == 2);
// 	getKeyOfSlotInLeaf(page, 2, (void*)&tmpkey, tmprid);
// 	assert(tmpkey == 75);
// 	assert(tmprid.pageNum == 20);
// 	assert(tmprid.slotNum == 15);
// 	getKeyOfSlotInLeaf(page, 3, (void*)&tmpkey, tmprid);
// 	assert(tmpkey == 100);
// 	assert(tmprid.pageNum == 5);
// 	assert(tmprid.slotNum == 10);


// 	// insert duplicate
// 	// insert (75, (40, 23))
// 	key = 75;
// 	rid.pageNum = 40;
// 	rid.slotNum = 23;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == 1);
// 	num_slots++;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));

// 	// insert (75, (100, 22))
// 	key = 75;
// 	rid.pageNum = 100;
// 	rid.slotNum = 22;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == 2);
// 	num_slots++;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));

// 	getKeyOfSlotInLeaf(page, 4, (void*)&tmpkey, tmprid);
// 	assert(tmpkey == 75);
// 	assert(tmprid.pageNum == 40);
// 	assert(tmprid.slotNum == 23);


// 	getKeyOfSlotInLeaf(page, 3, (void*)&tmpkey, tmprid);
// 	assert(tmpkey == 75);
// 	assert(tmprid.pageNum == 100);
// 	assert(tmprid.slotNum == 22);

// 	rc = PageFileHandle.WritePage(0, page);
// 	assert(rc == success);
// 	free(page);

// 	rc = pfm->CloseFile(this->PageFileHandle);
// 	assert(rc == success);

// 	return 0;
// }

// RC IX_IndexHandle::testDeleteLeaf() {
// 	RC rc;
// 	RC success = 0;
// 	char *testfilename = "testfile";
// 	remove(testfilename);
// 	this->IndexType = TypeInt;
// 	PF_Manager *pfm = PF_Manager::Instance();
// 	rc = pfm->CreateFile(testfilename);
// 	assert(rc == success);
// 	rc = pfm->OpenFile(testfilename, this->PageFileHandle);
// 	assert(rc == success);
// 	void *page = malloc(PF_PAGE_SIZE);
// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	rc = PageFileHandle.ReadPage(0, page);
// 	assert(rc == success);

// 	rc = initPage(page, IX_LEAF);
// 	assert(rc == success);

// 	int num_slots = 0;

// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));

// 	// insert (5, (1, 2))
// 	int key = 5;
// 	RID rid;
// 	rid.pageNum = 1;
// 	rid.slotNum = 2;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);
// 	num_slots++;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));

// 	int tmpkey;
// 	RID tmprid;
// 	getKeyOfSlotInLeaf(page, 0, (void*)&tmpkey, tmprid);
// 	assert(tmpkey == key);
// 	assert(tmprid.pageNum == rid.pageNum);
// 	assert(tmprid.slotNum == rid.slotNum);


// 	// insert (3, (2, 3))
// 	key = 3;
// 	rid.pageNum = 2;
// 	rid.slotNum = 3;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);
// 	num_slots++;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));

// 	// insert (100, (5, 10))
// 	key = 100;
// 	rid.pageNum = 5;
// 	rid.slotNum = 10;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);
// 	num_slots++;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));

// 	// insert (75, (20, 15))
// 	key = 75;
// 	rid.pageNum = 20;
// 	rid.slotNum = 15;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);
// 	num_slots++;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));


// 	getKeyOfSlotInLeaf(page, 0, (void*)&tmpkey, tmprid);

// 	assert(tmpkey == 3);
// 	assert(tmprid.pageNum == 2);
// 	assert(tmprid.slotNum == 3);
// 	getKeyOfSlotInLeaf(page, 1, (void*)&tmpkey, tmprid);
// 	assert(tmpkey == 5);
// 	assert(tmprid.pageNum == 1);
// 	assert(tmprid.slotNum == 2);
// 	getKeyOfSlotInLeaf(page, 2, (void*)&tmpkey, tmprid);
// 	assert(tmpkey == 75);
// 	assert(tmprid.pageNum == 20);
// 	assert(tmprid.slotNum == 15);
// 	getKeyOfSlotInLeaf(page, 3, (void*)&tmpkey, tmprid);
// 	assert(tmpkey == 100);
// 	assert(tmprid.pageNum == 5);
// 	assert(tmprid.slotNum == 10);


// 	// insert duplicate
// 	// insert (75, (40, 23))
// 	key = 75;
// 	rid.pageNum = 40;
// 	rid.slotNum = 23;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == 1);
// 	num_slots++;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));

// 	// insert (75, (100, 22))
// 	key = 75;
// 	rid.pageNum = 100;
// 	rid.slotNum = 22;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == 2);
// 	num_slots++;
// 	assert(num_slots == getSlotNumber(page));
// 	assert(4 + num_slots*12 == getFreeOffset(page));
// 	assert(PF_PAGE_SIZE - 6 - num_slots*2 == getSlotOffset(page));
// 	assert(PF_PAGE_SIZE - 10 - num_slots*14 == getFreeSpace(page));

// 	getKeyOfSlotInLeaf(page, 4, (void*)&tmpkey, tmprid);
// 	assert(tmpkey == 75);
// 	assert(tmprid.pageNum == 40);
// 	assert(tmprid.slotNum == 23);


// 	getKeyOfSlotInLeaf(page, 3, (void*)&tmpkey, tmprid);
// 	assert(tmpkey == 75);
// 	assert(tmprid.pageNum == 100);
// 	assert(tmprid.slotNum == 22);

// 	rc = PageFileHandle.WritePage(0, page);
// 	assert(rc == success);

// 	rc = pfm->CloseFile(this->PageFileHandle);
// 	assert(rc == success);

// 	printf("Keys: ");
// 	for (short i = 0; i < getSlotNumber(page); i++) {
// 		getKeyOfSlotInLeaf(page, i, &key, rid);
// 		printf("%d(%d, %d) ", key, rid.pageNum, rid.slotNum);
// 	}
// 	printf("\n");

// 	rc = deleteSlotInLeaf(page, 6);
// 	assert(rc != success);

// 	rc = deleteSlotInLeaf(page, 0);
// 	assert(rc == success);

// 	printf("Delete 3, Keys: ");
// 	for (short i = 0; i < getSlotNumber(page); i++) {
// 		getKeyOfSlotInLeaf(page, i, &key, rid);
// 		printf("%d(%d, %d) ", key, rid.pageNum, rid.slotNum);
// 	}
// 	printf("\n");

// 	rc = deleteSlotInLeaf(page, 4);
// 	assert(rc == success);

// 	printf("Delete 100, Keys: ");
// 	for (short i = 0; i < getSlotNumber(page); i++) {
// 		getKeyOfSlotInLeaf(page, i, &key, rid);
// 		printf("%d(%d, %d) ", key, rid.pageNum, rid.slotNum);
// 	}
// 	printf("\n");

// 	rc = deleteSlotInLeaf(page, 2);
// 	assert(rc == success);

// 	printf("Delete 75(100,22), Keys: ");
// 	for (short i = 0; i < getSlotNumber(page); i++) {
// 		getKeyOfSlotInLeaf(page, i, &key, rid);
// 		printf("%d(%d, %d) ", key, rid.pageNum, rid.slotNum);
// 	}
// 	printf("\n");

// 	free(page);
// 	return 0;
// }

// RC IX_IndexHandle::testScanInt() {
// 	RC rc;
// 	RC success = 0;
// 	char *testfilename = "testfile";
// 	remove(testfilename);
// 	this->IndexType = TypeInt;
// 	PF_Manager *pfm = PF_Manager::Instance();
// 	rc = pfm->CreateFile(testfilename);
// 	assert(rc == success);
// 	rc = pfm->OpenFile(testfilename, this->PageFileHandle);
// 	assert(rc == success);
// 	void *page = malloc(PF_PAGE_SIZE);
// 	rc = initPage(page, IX_NODE);
// 	assert(rc == success);

// 	// insert 3 (key,ptr) tuples
// 	int key = 5;
// 	PageNum pn = 1;
// 	rc = addKeyToNode(page, (void*)&key, pn);
// 	assert(rc == success);

// 	key = 15;
// 	pn = 2;
// 	rc = addKeyToNode(page, (void*)&key, pn);
// 	assert(rc == success);

// 	key = 10;
// 	pn = 3;
// 	rc = addKeyToNode(page, (void*)&key, pn);
// 	assert(rc == success);

// 	rc = setExtraPtr(page, 4);
// 	assert(rc == success);

// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	// prepare 4 leaf pages
	
// 	/* page 1: (5, 10)
// 	   3 keys
// 	   (7, (3, 2))
// 	   (6, (4, 15))
// 	   (5, (2, 26))
// 	*/
// 	free(page);
// 	page = malloc(PF_PAGE_SIZE);

// 	rc = initPage(page, IX_LEAF);
// 	assert(rc == success);

// 	RID rid;
// 	key = 7;
// 	rid.pageNum = 3;
// 	rid.slotNum = 2;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 6;
// 	rid.pageNum = 4;
// 	rid.slotNum = 15;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 5;
// 	rid.pageNum = 2;
// 	rid.slotNum = 26;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	rc = setExtraPtr(page, 3);
// 	assert(rc == success);

// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	/* page 2: (15, )
// 	   3 keys
// 	   (16, (9, 3))
// 	   (16, (7, 0))
// 	   (19, (10, 56))
// 	*/
// 	free(page);
// 	page = malloc(PF_PAGE_SIZE);

// 	rc = initPage(page, IX_LEAF);
// 	assert(rc == success);

// 	key = 16;
// 	rid.pageNum = 9;
// 	rid.slotNum = 3;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 16;
// 	rid.pageNum = 7;
// 	rid.slotNum = 0;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	//assert(rc == success);

// 	key = 19;
// 	rid.pageNum = 10;
// 	rid.slotNum = 56;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	rc = setExtraPtr(page, 0);
// 	assert(rc == success);

// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	/* page 3: (10, 15)
// 	   3 keys
// 	   (10, (98, 2))
// 	   (14, (7, 67))
// 	   (12, (30, 5))
// 	*/
// 	free(page);
// 	page = malloc(PF_PAGE_SIZE);

// 	rc = initPage(page, IX_LEAF);
// 	assert(rc == success);

// 	key = 10;
// 	rid.pageNum = 98;
// 	rid.slotNum = 2;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 14;
// 	rid.pageNum = 7;
// 	rid.slotNum = 67;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 12;
// 	rid.pageNum = 30;
// 	rid.slotNum = 5;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	rc = setExtraPtr(page, 2);
// 	assert(rc == success);

// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	/* page 4: (, 5)
// 	   3 keys
// 	   (-5, (34, 27))
// 	   (3, (76, 37))
// 	   (1, (32, 8))
// 	*/
// 	free(page);
// 	page = malloc(PF_PAGE_SIZE);

// 	rc = initPage(page, IX_LEAF);
// 	assert(rc == success);

// 	key = -5;
// 	rid.pageNum = 34;
// 	rid.slotNum = 27;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 3;
// 	rid.pageNum = 76;
// 	rid.slotNum = 37;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 1;
// 	rid.pageNum = 32;
// 	rid.slotNum = 8;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	rc = setExtraPtr(page, 1);
// 	assert(rc == success);

// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	free(page);

// 	/* all keys
// 		 (-5, (34, 27))
// 	   (1, (32, 8))
// 	   (3, (76, 37))
// 	   (5, (2, 26))	   
// 	   (6, (4, 15))	   
// 	   (7, (3, 2))
// 	   (10, (98, 2))
// 	   (12, (30, 5))	 	   
// 	   (14, (7, 67))
// 	   (16, (9, 3))
// 	   (16, (7, 0))
// 	   (19, (10, 56))
// 	*/

// 	IX_IndexScan scan;
// 	int k = 0;
// 	void *value = malloc(PF_PAGE_SIZE);
// 	memcpy(value, &k, 4);
// 	scan.OpenScan(*this, NE_OP, value);	
// 	while (scan.GetNextEntry(rid) != IX_EOF) {
// 		printf("(%d %d)\n", rid.pageNum, rid.slotNum);
// 	}
// 	return 0;
// }

// RC IX_IndexHandle::testScanFloat() {
// 	RC rc;
// 	RC success = 0;
// 	char *testfilename = "testfile";
// 	remove(testfilename);
// 	this->IndexType = TypeReal;
// 	PF_Manager *pfm = PF_Manager::Instance();
// 	rc = pfm->CreateFile(testfilename);
// 	assert(rc == success);
// 	rc = pfm->OpenFile(testfilename, this->PageFileHandle);
// 	assert(rc == success);
// 	void *page = malloc(PF_PAGE_SIZE);
// 	rc = initPage(page, IX_NODE);
// 	assert(rc == success);

// 	// insert 3 (key,ptr) tuples
// 	float key = 30.3;
// 	PageNum pn = 1;
// 	rc = addKeyToNode(page, (void*)&key, pn);
// 	assert(rc == success);

// 	key = 100.38;
// 	pn = 2;
// 	rc = addKeyToNode(page, (void*)&key, pn);
// 	assert(rc == success);

// 	key = 50.778;
// 	pn = 3;
// 	rc = addKeyToNode(page, (void*)&key, pn);
// 	assert(rc == success);

// 	rc = setExtraPtr(page, 4);
// 	assert(rc == success);

// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	// prepare 4 leaf pages
	
// 	/* page 1: (30.3, 50.778)
// 	   3 keys
// 	   (40.1, (40, 1))
// 	   (50.68, (50, 68))
// 	   (36.12, (36, 12))
// 	*/
// 	free(page);
// 	page = malloc(PF_PAGE_SIZE);

// 	rc = initPage(page, IX_LEAF);
// 	assert(rc == success);

// 	RID rid;
// 	key = 40.1;
// 	rid.pageNum = 40;
// 	rid.slotNum = 1;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 50.68;
// 	rid.pageNum = 50;
// 	rid.slotNum = 68;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 36.12;
// 	rid.pageNum = 36;
// 	rid.slotNum = 12;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	rc = setExtraPtr(page, 3);
// 	assert(rc == success);

// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	/* page 2: (100.38, )
// 	   3 keys
// 	   (200.282, (200, 282))
// 	   (161.12, (161, 12))
// 	   (190.333, (190, 333))
// 	*/
// 	free(page);
// 	page = malloc(PF_PAGE_SIZE);

// 	rc = initPage(page, IX_LEAF);
// 	assert(rc == success);

// 	key = 200.282;
// 	rid.pageNum = 200;
// 	rid.slotNum = 282;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 161.12;
// 	rid.pageNum = 161;
// 	rid.slotNum = 12;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	//assert(rc == success);

// 	key = 190.333;
// 	rid.pageNum = 190;
// 	rid.slotNum = 333;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	rc = setExtraPtr(page, 0);
// 	assert(rc == success);

// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	/* page 3: (50.778, 100.38)
// 	   3 keys
// 	   (88.88, (88, 88))
// 	   (92.22, (92, 22))
// 	   (92.22, (92, 22))
// 	*/
// 	free(page);
// 	page = malloc(PF_PAGE_SIZE);

// 	rc = initPage(page, IX_LEAF);
// 	assert(rc == success);

// 	key = 88.88;
// 	rid.pageNum = 88;
// 	rid.slotNum = 88;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 92.22;
// 	rid.pageNum = 92;
// 	rid.slotNum = 22;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 92.22;
// 	rid.pageNum = 92;
// 	rid.slotNum = 22;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	//assert(rc == success);

// 	rc = setExtraPtr(page, 2);
// 	assert(rc == success);

// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	/* page 4: (, 30.3)
// 	   3 keys
// 	   (10.11, (10, 11))
// 	   (15.96, (15, 96))
// 	   (22.67, (22, 67))
// 	*/
// 	free(page);
// 	page = malloc(PF_PAGE_SIZE);

// 	rc = initPage(page, IX_LEAF);
// 	assert(rc == success);

// 	key = 10.11;
// 	rid.pageNum = 10;
// 	rid.slotNum = 11;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 15.96;
// 	rid.pageNum = 15;
// 	rid.slotNum = 96;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	key = 22.67;
// 	rid.pageNum = 22;
// 	rid.slotNum = 67;
// 	rc = addKeyToLeaf(page, (void*)&key, rid);
// 	assert(rc == success);

// 	rc = setExtraPtr(page, 1);
// 	assert(rc == success);

// 	rc = PageFileHandle.AppendPage(page);
// 	assert(rc == success);

// 	free(page);

// 	/* all keys
// 	   (10.11, (10, 11))
// 	   (15.96, (15, 96))
// 	   (22.67, (22, 67))
// 	   (36.12, (36, 12))	   
// 	   (40.1, (40, 1))
// 	   (50.68, (50, 68))
// 	   (88.88, (88, 88))
// 	   (92.22, (92, 22))
// 	   (92.22, (92, 22))
// 	   (161.12, (161, 12))
// 	   (190.333, (190, 333))	   
// 	   (200.282, (200, 282))	   
// 	*/

// 	IX_IndexScan scan;
// 	float k = 92.22;
// 	void *value = malloc(PF_PAGE_SIZE);
// 	memcpy(value, &k, 4);
// 	scan.OpenScan(*this, GE_OP, value);	
// 	while (scan.GetNextEntry(rid) != IX_EOF) {
// 		printf("(%d %d)\n", rid.pageNum, rid.slotNum);
// 	}
// }

