
- Modify the "CODEROOT" variable in makefile.inc to point to the root
  of your code base

- Implement the Paged File component (PF):

   Go to folder "pf" and type in:

    make clean
    make
    ./pftest

   The program should work.  But it does nothing.  You are supposed to
   implement the API of the paged file manager defined in rm.h

- By default you should not change those functions of the PF_Manager and PF_FileHandle class defined in pf/pf.h. If you think some changes are really necessary, please contact us first.
