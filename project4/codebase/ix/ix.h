
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <deque>

#include "../pf/pf.h"
#include "../rm/rm.h"

#define IX_EOF (-1)  // end of the index scan
#define IX_NODE 0
#define IX_LEAF 1
#define IX_ORDER 120
#define IX_ROOT_PAGE_NUMBER 0
#define IX_FREE_SPACE 50

using namespace std;

class IX_IndexHandle;

class IX_Manager {
 public:
  static IX_Manager* Instance();

  RC CreateIndex(const string tableName,       // create new index
		 const string attributeName);
  RC DestroyIndex(const string tableName,      // destroy an index
		  const string attributeName);
  RC OpenIndex(const string tableName,         // open an index
	       const string attributeName,
	       IX_IndexHandle &indexHandle);
  RC CloseIndex(IX_IndexHandle &indexHandle);  // close index
  
 protected:
  IX_Manager   ();                             // Constructor
  ~IX_Manager  ();                             // Destructor
 
 private:
  static IX_Manager *_ix_manager;
  // added
  string INDEXDIR;
  PF_Manager* thisPFM;
  RM* thisRM;
};


class IX_IndexHandle {
public:
  IX_IndexHandle  ();                           // Constructor
  ~IX_IndexHandle ();                           // Destructor

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC InsertEntry(void *key, const RID &rid);  // Insert new index entry
  RC DeleteEntry(void *key, const RID &rid);  // Delete index entry

  // these are needed for index scan
  RC setIndexType(AttrType type);
  RC getPageOfKey(void *key, PageNum target, PageNum *pn, short *sn) const;
  RC getLeftmost(void *key, PageNum target, PageNum *pn) const;
  short getSlotNumber(const void *page) const;
  PageNum getExtraPtr(const void *page) const;
  RC getKeyOfSlotInLeaf(const void *page, short slot_num, void *key, RID &rid) const; 
  PF_FileHandle getFileHandle() const;
  int getPageType(const void *page) const;
  AttrType getIndexType() const;
  RC findPlaceToInsertInLeaf(const void *page, const void *key, short *slotnum) const;
  RC initRootNode();
  bool isOpened() const { return Opened; };

  // use these two functions in OpenIndex and CloseIndex
  RC openFileHandle(string filename);
  RC closeFileHandle();
  
  // RC testNode();
  // RC testNodeStr();
  // RC testLeaf();
  // RC testDeleteLeaf();
  // RC testLeafStr();
  // RC testScanInt();
  // RC testScanFloat();

  // should be set in OpenIndex
private:
  AttrType IndexType;
  PF_FileHandle PageFileHandle;
  bool Opened; 

  RC deleteSlotInLeaf(void *page, short slotnum);
  RC deleteSlotInNode(void *page, short slotnum);
  RC DeleteEntryFromNode(PageNum parent, PageNum current, PageNum leftnode, void *key, const RID &rid, void *key_to_remove, PageNum &page_to_remove);
  short findKeyPairInLeaf(void *page, void *key, const RID &rid);
  RC InsertEntryToNode(PageNum current, void *key, const RID &rid, void *newkey, PageNum &newpage);
  RC splitLeaf(PageNum original, void *key, const RID &rid, PageNum &new_append, void *middlekey);
  int compareKey(void *key1, void *key2);
  RC splitNode(PageNum original, void *key, PageNum pointer, PageNum &new_append, void *middlekey);
  RC createNewRoot(void *newkey, PageNum newpage);
  RC deleteKeyPairInNode(void *page, PageNum page_to_remove);

  bool shouldSplit(const void *page, const void *key);
  PageNum getKeyPtrInNode(const void *page, const void *key, short *slotnum) const; // search
  RID getKeyRidInLeaf(const void *page, const void *key, short *slotnum, int *num_dup) const; // search
  RC initPage(void *page, int type);
  RC setExtraPtr(void *page, PageNum pagenum);
  RC addKeyToNode(void *page, const void *key, PageNum pagenum);
  RC addKeyToLeaf(void *page, const void *key, const RID &rid);
  RC getKeyOfSlotInNode(const void *page, short slot_num, void *key, PageNum *pagenum) const;

  
  short getFreeOffset(const void *page);
  short getSlotOffset(const void *page);
  short getFreeSpace(const void *page);
  RC setFreeOffset(void *page, short offset);
  RC setSlotNumber(void *page, short num);
  RC setPageType(void *page, int type);
  
  
};


class IX_IndexScan {
public:
  IX_IndexScan(){};  								// Constructor
  ~IX_IndexScan(){}; 								// Destructor

  // for the format of "value", please see IX_IndexHandle::InsertEntry()
  RC OpenScan(const IX_IndexHandle &indexHandle, // Initialize index scan
	      CompOp      compOp,
	      void        *value);           

  RC GetNextEntry(RID &rid);  // Get next matching entry
  RC CloseScan();             // Terminate index scan

private:
  RC addRid(const RID &rid);
  deque<RID> *ridqueue;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif



// old insert

//  /****/
  // RC InsertEntryIntoNode(void * node, PageNum nodeNum, void * key, const RID & rid, void *newChildPointer);
  // template <typename T>
  // RC splitRegularLeaf(void * node, PageNum nodeNum,
  //     void * key, const RID & rid, void *newChildPointer) {
  //   T keyValue;
  //   memcpy(&keyValue, key, sizeof(T));

  //   //insert keyValue into leaf node
  //   RC duplicate = -1;
  //   short slotNum = this->getSlotNumber(node);
  //   T currentKey;
  //   for(short i = 0; i < slotNum; i++) {
  //     memcpy(&currentKey, (char*)node + 4 + i * 12, sizeof(T));
  //     if(keyValue > currentKey) {
  //       continue;
  //     } else { //notice duplicate keys
  //       for(int j = slotNum; j > i; j--) {
  //         memcpy((char*)node + 4 + 12 * j, (char*)node + 4 + 12*(j-1), 12);
  //       }
  //       memcpy((char*)node + 4 + 12 * i, key, sizeof(T));
  //       memcpy((char*)node + 4 + 12 * i + 4, &rid.pageNum, sizeof(unsigned));
  //       memcpy((char*)node + 4 + 12 * i + 8, &rid.slotNum, sizeof(unsigned));

  //       if((keyValue - currentKey) < 0.001 && (keyValue - currentKey) > -0.001) duplicate = 1;
  //       if(keyValue < currentKey) duplicate = 0;
  //       break;
  //     }
  //   }
  //   if(duplicate == -1) { //new entry should be the last one
  //     memcpy((char*)node + 4 + 12 * slotNum, key, sizeof(T));
  //     memcpy((char*)node + 4 + 12 * slotNum + 4, &rid.pageNum, sizeof(unsigned));
  //     memcpy((char*)node + 4 + 12 * slotNum + 8, &rid.slotNum, sizeof(unsigned));
  //     duplicate = 0;
  //   }

  //   // copy the latter half of node(ORDER + 1 entries) to newNode
  //   void * newNode = calloc(PF_PAGE_SIZE, 1);
  //   this->initPage(newNode, IX_LEAF);
  //   for(int i = IX_ORDER, j = 0; i <= 2 * IX_ORDER; i++, j++) {
  //     memcpy((char*)newNode + 4 + 12 * j, (char *)node + 4 + 12 * i, 12);
  //   }

  //   //init newNode,including forward pointer, slotNum, freeOffset
  //   this->setSlotNumber(newNode, (short)IX_ORDER + 1);
  //   this->setFreeOffset(newNode, (short)(4 + (IX_ORDER + 1)*12));
  //   PageNum nextNodePointer = this->getExtraPtr(node);
  //   this->setExtraPtr(newNode, nextNodePointer);

  //   //insert new node into index
  //   this->PageFileHandle.AppendPage(newNode);

  //   //configure the newChildEntry
  //   PageNum newChildEntryPointer = this->PageFileHandle.GetNumberOfPages() - 1;
  //   int newChildEntryValue;
  //   memcpy(&newChildEntryValue, (char*)newNode + 4, sizeof(T));
  //   newChildPointer = malloc(sizeof(T) + sizeof(unsigned));
  //   memcpy(newChildPointer, &newChildEntryValue, sizeof(T));
  //   memcpy((char*)newChildPointer + 4, &newChildEntryPointer, sizeof(unsigned));

  //   //configure the node, write it back
  //   this->setExtraPtr(node, newChildEntryPointer);
  //   this->setSlotNumber(node, IX_ORDER);
  //   this->setFreeOffset(node, (short)(4 + 12 * IX_ORDER) );
  //   this->PageFileHandle.WritePage(nodeNum, node);

  //   return duplicate;
  // }

  // template <typename T>
  // RC splitRegularNode(void * node, PageNum nodeNum,
  //     void * key, const RID & rid, void *newChildPointer) {
  //   T newChildValue;
  //   PageNum newChildPage;
  //   memcpy(&newChildValue, newChildPointer, sizeof(T));
  //   memcpy(&newChildPage, newChildPointer, sizeof(PageNum));
  //   //add one more entry into node. The node size now is 2d + 1, 2d + 2
  //   this->addKeyToNode(node, &newChildValue, newChildPage);

  //   void * leftHalfNode = calloc(PF_PAGE_SIZE, 1);
  //   this->initPage(leftHalfNode, IX_NODE);
  //   void * rightHalfNode = calloc(PF_PAGE_SIZE, 1);
  //   this->initPage(rightHalfNode, IX_NODE);

  //   //copy the first d keys and d + 1 pointers to leftHalfNode
  //   memcpy(leftHalfNode, (char*)node, 4 * (IX_ORDER + IX_ORDER + 1) );
  //   this->setSlotNumber(leftHalfNode, IX_ORDER);
  //   this->setFreeOffset(leftHalfNode, 4 * (IX_ORDER + IX_ORDER + 1));

  //   //copy the last d keys  and d + 1 pointers to rightHalfNode
  //   memcpy(rightHalfNode, (char*)node + 4 + 4 * (IX_ORDER + IX_ORDER + 1), 4 * (IX_ORDER + IX_ORDER + 1));
  //   this->setSlotNumber(rightHalfNode, IX_ORDER);
  //   this->setFreeOffset(rightHalfNode, 4*(IX_ORDER + IX_ORDER + 1));

  //   //record the d + 1 key value
  //   T middleKey;
  //   memcpy(&middleKey, (char*)node + 4*(IX_ORDER + IX_ORDER + 1), sizeof(T));

  //   //insert the rightHalfNode to index file
  //   this->PageFileHandle.AppendPage(rightHalfNode);
  //   PageNum rightHalfPageNum = this->PageFileHandle.GetNumberOfPages() - 1;

  //   //configure newChildPointer
  //   newChildPointer = malloc(sizeof(T) + sizeof(PageNum));
  //   memcpy(newChildPointer, &middleKey, sizeof(T));
  //   // TODO: +4?
  //   memcpy((char*)newChildPointer, &rightHalfPageNum, sizeof(PageNum));

  //   //write back
  //   if(nodeNum != 0) { //this splited node is not root
  //     PageFileHandle.WritePage(nodeNum, leftHalfNode);
  //   }
  //   if(nodeNum == 0) { //this split node is root
  //     PageFileHandle.AppendPage(leftHalfNode);
  //     PageNum leftHalfPageNum = this->PageFileHandle.GetNumberOfPages() - 1;
  //     void * newRoot = calloc(PF_PAGE_SIZE, 1);
  //     this->initPage(newRoot, IX_NODE);
  //     this->setExtraPtr(newRoot, leftHalfPageNum);

  //     this->addKeyToNode(newRoot, &middleKey, rightHalfPageNum);

  //     PageFileHandle.WritePage(0, newRoot);

  //     free(newRoot);
  //   }
  //   free(leftHalfNode);
  //   free(rightHalfNode);
  //   return 0;
  // }
