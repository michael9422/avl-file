/* avl_file.h
 * 
 * These routines implement file-based threaded AVL-trees with multiple
 * keys and concurrent access, using fixed length records in single files.
 *
 *-----------------------------------------------------------------------
 * Copyright (C) 2007-2009 Michael Williamson <michael.h.williamson@gmail.com>
 *
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *-----------------------------------------------------------------------
 *
 *
 * The format for the files has some header data first, followed by
 * AVL-tree records, as well as current-pointer records (one for each
 * call to avl_file_open), and also empty records, interspersed.
 * The header structure contains pointers to the AVL-tree root records,
 * and also singly linked list head pointers to lists for current-pointer
 * records and empty records. The AVL-tree records are also in a doubly
 * linked list for unordered sequential access.
 *
 * The comparison function, which is passed as a pointer to these routines,
 * cannot be changed once an AVL-tree file has been created, obviously. Nor
 * can the data format be changed. Duplicate keys are allowed.
 *
 * A file will be left in a corrupted state if the functions are
 * interrupted before completing. There is no provision for identifying
 * or repairing a corrupted file. The functions will call abort()
 * if a corrupted file causes a read or write beyond the end of file.
 *
 *
 *
 *
 *
 *--------------------------------------------------------------------
 * The functions are:
 *
 *    avl_file_open ()          - open
 *    avl_file_close ()         - close
 *    avl_file_getnum ()        - get a sequential record number
 *    avl_file_startseq ()      - position sequential pointer to start
 *    avl_file_readseq ()       - read the next record sequentially
 *    avl_file_insert ()        - insert a new record
 *    avl_file_update ()        - update a record
 *    avl_file_delete ()        - delete a record
 *    avl_file_startlt ()       - read the first record less than key
 *    avl_file_startge ()       - read record greater or equal to key
 *    avl_file_next ()          - read the next record by key
 *    avl_file_prev ()          - read the previous record by key
 *    avl_file_find ()          - get a record by key
 *    avl_file_scan ()          - scan the tree recursively by key
 *    avl_file_lock ()          - lock the file for exclusive access
 *    avl_file_unlock ()        - unlock the file
 *    avl_file_dump ()          - show tree nodes
 *    avl_file_squash ()        - eliminate empty records, shorten file
 *
 * Note: Thread-safe versions of the functions have '_t' appended to the
 *       end of the function names, i.e. avl_file_open_t (),
 *       avl_file_insert_t (), etc.
 *
 *--------------------------------------------------------------------
 * An example program using two keys:
 *
 *    #include <avl_file.h>
 *    ...
 *
 *    #define RK_NUM               0     // key IDs (0, 1, etc...)
 *    #define RK_OBJ_NUM_REG       1
 *
 *    struct r_struct {    // fixed data structure (including key fields)
 *       int32_t num;
 *       char object[24];
 *       int32_t reg;
 *       char data[100];
 *    };
 *
 *    ...
 *
 *    int32_t cmp_test (int32_t key, void *va, void *vb) {    // comparison function
 *       struct r_struct *a, *b;
 *       int32_t i;
 *
 *       a = (struct r_struct *) va;
 *       b = (struct r_struct *) vb;
 *
 *       switch (key) {
 *       case RK_NUM:
 *          i = a->num - b->num;
 *          break;
 *       case RK_OBJ_NUM_REG:
 *          i = strcmp (a->object, b->object);
 *          if (i == 0) i = a->num - b->num;
 *          if (i == 0) i = a->reg - b->reg;
 *          break;
 *       default:
 *          fprintf (stderr, "cmp_r: invalid key\n");
 *          break;
 *       }
 *       return (i);
 *    }
 *
 *    ...
 *
 *    int main (int argc, char *argv[]) {
 *       AVL_FILE *ap;
 *       struct r_struct r;
 *       int32_t n;
 *
 *       //--- avl_file_open (fname, rec size, n_keys, cmp function)
 *       ap = avl_file_open ("test.avl", sizeof (struct r_struct),
 *                           2, (avl_file_cmp_fn_t) cmp_test);
 *
 *       r.num = 1;
 *       strcpy (r.object, "GNU/Linux");
 *       r.reg = 0;
 *       strcpy (r.data, "SuSE");
 *       avl_file_insert (ap, &r);
 *
 *       ...
 *
 *       r.num = 1;
 *       n = avl_file_startge (ap, &r, RK_NUM);
 *       while (n == 0) {
 *          if (r.num > 1) break;
 *
 *          printf ("%s %s\n", r.object, r.data);
 *          n = avl_file_next (ap, &r, RK_NUM);
 *       }
 *
 *       avl_file_close (ap);
 *    }
 *
 *---------------------------------------------------------------------------
 *
 */

#ifndef AVL_FILE_H
#define AVL_FILE_H     1


#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h> 
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <semaphore.h>		// semaphores require a threads library


struct avl_node_struct {
   char b;		// balance
   off_t l, r;		// left/right pointers
};

typedef int32_t (*avl_file_cmp_fn_t) (int32_t, const void *, const void *);

struct avl_file_struct { 
   char *fname;
   int32_t fd, n_keys, len, reclen;	// len is the data structure length
   avl_file_cmp_fn_t cmp;
   off_t cpr;
   sem_t sem;		// serialize process-thread file position access
};

typedef struct avl_file_struct AVL_FILE;



/*
 * AVL function prototypes
 */

AVL_FILE *avl_file_open (char *fname, int32_t len, int32_t n_keys, avl_file_cmp_fn_t cmp);
void      avl_file_close (AVL_FILE *avl_fp);
int64_t   avl_file_getnum (AVL_FILE *avl_fp);
void      avl_file_startseq (AVL_FILE *avl_fp);
int32_t   avl_file_readseq (AVL_FILE *avl_fp, void *data);
int32_t   avl_file_insert (AVL_FILE *avl_fp, void *data);
int32_t   avl_file_delete (AVL_FILE *avl_fp, void *data);
int32_t   avl_file_update (AVL_FILE *avl_fp, void *data);
int32_t   avl_file_startlt (AVL_FILE *avl_fp, void *data, int32_t k);
int32_t   avl_file_startge (AVL_FILE *avl_fp, void *data, int32_t k);
int32_t   avl_file_next (AVL_FILE *avl_fp, void *data, int32_t k);
int32_t   avl_file_prev (AVL_FILE *avl_fp, void *data, int32_t k);
int32_t   avl_file_find (AVL_FILE *avl_fp, void *data, int32_t k);
int32_t   avl_file_scan (AVL_FILE *avl_fp, int32_t k, off_t sp, int64_t *count);
void      avl_file_lock (AVL_FILE *avl_fp);
void      avl_file_unlock (AVL_FILE *avl_fp);
void      avl_file_dump (AVL_FILE *avl_fp);
void      avl_file_squash (AVL_FILE *avl_fp);


/*
 * AVL thread-safe function prototypes
 */

AVL_FILE *avl_file_open_t (char *fname, int32_t len, int32_t n_keys, avl_file_cmp_fn_t cmp);
void      avl_file_close_t (AVL_FILE *avl_fp);
int64_t   avl_file_getnum_t (AVL_FILE *avl_fp);
void      avl_file_startseq_t (AVL_FILE *avl_fp);
int32_t   avl_file_readseq_t (AVL_FILE *avl_fp, void *data);
int32_t   avl_file_insert_t (AVL_FILE *avl_fp, void *data);
int32_t   avl_file_delete_t (AVL_FILE *avl_fp, void *data);
int32_t   avl_file_update_t (AVL_FILE *avl_fp, void *data);
int32_t   avl_file_startlt_t (AVL_FILE *avl_fp, void *data, int32_t k);
int32_t   avl_file_startge_t (AVL_FILE *avl_fp, void *data, int32_t k);
int32_t   avl_file_next_t (AVL_FILE *avl_fp, void *data, int32_t k);
int32_t   avl_file_prev_t (AVL_FILE *avl_fp, void *data, int32_t k);
int32_t   avl_file_find_t (AVL_FILE *avl_fp, void *data, int32_t k);
int32_t   avl_file_scan_t (AVL_FILE *avl_fp, int32_t k, off_t sp, int64_t *count);
void      avl_file_lock_t (AVL_FILE *avl_fp);
void      avl_file_unlock_t (AVL_FILE *avl_fp);
void      avl_file_dump_t (AVL_FILE *avl_fp);
void      avl_file_squash_t (AVL_FILE *avl_fp);


#define	AVL_FILE_EMSG_VNAME	"AVL_FILE_EMSG"	/* error message environment variable */

#endif /* AVL_FILE_H */
