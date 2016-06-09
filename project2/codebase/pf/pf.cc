#include "pf.h"
#include <cstdio>

PF_Manager* PF_Manager::_pf_manager = 0;


PF_Manager* PF_Manager::Instance()
{
    if(!_pf_manager)
        _pf_manager = new PF_Manager();
    
    return _pf_manager;    
}


PF_Manager::PF_Manager()
{
}


PF_Manager::~PF_Manager()
{
}

    
RC PF_Manager::CreateFile(const char *fileName)
{
    FILE *file = fopen(fileName, "rb");
    if (file != NULL) return -1; // File already exists
    file = fopen(fileName, "wb");
    if (file != NULL) {
        fclose(file);
        return 0;
    }
    return -1;
}


RC PF_Manager::DestroyFile(const char *fileName)
{ 
    FILE *file = fopen(fileName, "rb");
    if (file != NULL) {
        fclose(file);
        remove(fileName);
        return 0;
    }
    return -1;
}


RC PF_Manager::OpenFile(const char *fileName, PF_FileHandle &fileHandle)
{
    FILE *file = fopen(fileName, "r+b");
    if (file == NULL) return -1; // File does not exist
    
    if (fileHandle.GetFile() != NULL) return -1; // fileHandle is handling another file

    fseek(file, 0, SEEK_END);
    int size = ftell(file);
    if (size % PF_PAGE_SIZE != 0) return -1; // not created by createfile?
    fileHandle.SetNumberOfPages(size / PF_PAGE_SIZE);
    fileHandle.SetFile(file);
    return 0;
}


RC PF_Manager::CloseFile(PF_FileHandle &fileHandle)
{
    // check the filehandle?
    if (fileHandle.GetFile() == NULL) return -1;

    fileHandle.ExitHandle();
    return 0;
}


int PF_FileHandle::Reset()
{
    current_pages = 0;
    fileptr = NULL;
    return 0;
}

PF_FileHandle::PF_FileHandle()
{
    Reset();
}
 

PF_FileHandle::~PF_FileHandle()
{
    //free(data_pages);
}


RC PF_FileHandle::ReadPage(PageNum pageNum, void *data)
{
    if (pageNum < current_pages) {
        fseek(fileptr, pageNum * PF_PAGE_SIZE, SEEK_SET);
        fread(data, 1, PF_PAGE_SIZE, fileptr);
        return 0;
    }
    return -1;
}


RC PF_FileHandle::WritePage(PageNum pageNum, const void *data)
{
    if (pageNum < current_pages) {
        fseek(fileptr, pageNum * PF_PAGE_SIZE, SEEK_SET);
        fwrite(data, 1, PF_PAGE_SIZE, fileptr);
        fflush(fileptr);
        return 0;
    }
    return -1;
}


RC PF_FileHandle::AppendPage(const void *data)
{
    // if (current_buffer_used >= current_buffer_size) {
    //     void *new_buffer = malloc(PF_PAGE_SIZE * current_buffer_size * 2);
    //     memcpy(new_buffer, data_pages, current_buffer_size * PF_PAGE_SIZE);
    //     free(data_pages);
    //     data_pages = new_buffer;
    //     current_buffer_size *= 2;
    // }
    fseek(fileptr, 0, SEEK_END);
    fwrite(data, 1, PF_PAGE_SIZE, fileptr);
    fflush(fileptr);
    current_pages++;
    return 0;
}


unsigned PF_FileHandle::GetNumberOfPages()
{
    return current_pages;
}

RC PF_FileHandle::SetNumberOfPages(PageNum num)
{
    current_pages = num;
    return 0;
}

RC PF_FileHandle::SetFile(FILE *fp)
{
    fileptr = fp;
    return 0;
}

FILE *PF_FileHandle::GetFile()
{
    return fileptr;
}

// int PF_FileHandle::SetFileName(const char *name)
// {
//     filename = string(name);
//     return 0;
// }

int PF_FileHandle::ExitHandle()
{
    fflush(fileptr);
    fclose(fileptr);
    Reset();
    return 0;
}

// int PF_FileHandle::FlushData()
// {
//     FILE *file = fopen(filename.c_str(), "wb");
//     for (unsigned i = 0; i < current_buffer_used; i++) {
//         fwrite((char *)data_pages + i * PF_PAGE_SIZE, 1, PF_PAGE_SIZE, file);
//     }
//     fclose(file);
//     return 0;
// }

// string PF_FileHandle::GetFileName() {
//     return filename;
// }
