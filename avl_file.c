/* avl_file.c
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
 *  2009-11-21  Added the preprocessor macro AVL_FILE_TSAFE for
 *              compiling thread-safe versions of the functions.
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

#include "config.h"
#include "avl_file.h"





/*------------------------------------------- avl_file_fatal
 * Abort because of an unrecoverable error.
 */
static void 
avl_file_fatal (AVL_FILE *avl_fp, char *s)
{
   setenv (AVL_FILE_EMSG_VNAME, s, 1);
   abort ();
}


/*------------------------------------------- avl_file_lread
 * This function should only be called by other avl_file functions.
 */
static void
avl_file_lread (AVL_FILE *avl_fp, off_t *lim, off_t pos, void *pr, int32_t len)
{
   if (pos > *lim) avl_file_fatal (avl_fp, "10 corrupted file, seek pos > lim");
   if (lseek (avl_fp->fd, pos, SEEK_SET) != pos) avl_file_fatal (avl_fp, "11 lseek failed");
   if (read (avl_fp->fd, pr, len) != len) avl_file_fatal (avl_fp, "12 read failed");
}


/*------------------------------------------- avl_file_lwrite
 * This function should only be called by other avl_file functions.
 */
static void
avl_file_lwrite (AVL_FILE *avl_fp, off_t *lim, off_t pos, void *pr, int32_t len)
{
   if (pos > *lim) avl_file_fatal (avl_fp, "13 corrupted file, seek pos > lim");
   if (lseek (avl_fp->fd, pos, SEEK_SET) != pos) avl_file_fatal (avl_fp, "14 lseek failed");
   if (write (avl_fp->fd, pr, len) != len) avl_file_fatal (avl_fp, "15 write failed");
   if (pos + len > *lim) *lim = pos + len;
}




/*------------------------------------------- avl_file_open
 * Opens an AVL file for reading and writing. The len parameter
 * sets the (fixed) data length, and the data buffer passed to
 * the other avl_file_xxx() functions must be the same size.
 *
 * The value n_keys and the comparison function cmp() must be 
 * the same for future calls once an AVL file has been created.
 *
 * The first byte of the file is used to ensure exclusive
 * access for each of the avl_file_xxx() functions by locking 
 * it during those routines.
 */
AVL_FILE *
#ifdef	AVL_FILE_TSAFE
avl_file_open_t (char *fname, int32_t len, int32_t n_keys, avl_file_cmp_fn_t cmp)
#else
avl_file_open (char *fname, int32_t len, int32_t n_keys, avl_file_cmp_fn_t cmp)
#endif
{
   AVL_FILE *avl_fp, avl_dummy;
   int32_t fd, n, i, reclen;

   struct hdr_struct {
      char magic[8];
      int32_t n_keys; 
      int32_t len;        // AVL record data length
      int32_t reclen;     // AVL record length including key nodes
      int64_t n_avl;      // number of AVL records 
      int64_t nextnum;    // unique record numbers 
      off_t root[n_keys];
      off_t head_seq;     // doubly linked
      off_t head_empty;   // singly linked
      off_t head_cpr;     // singly linked
   } hdr;

   struct avl_struct {
      struct avl_node_struct n[n_keys];
      off_t prev, next;   // prev used as sequential pointer in 'cpr' records
      char b[len];
   } cpr;                 // per-process current position pointer
   off_t cp, lim;
   pid_t pid;


   avl_fp = NULL;
   reclen = sizeof (struct avl_struct);

   unsetenv (AVL_FILE_EMSG_VNAME);
   fd = open (fname, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
   if (fd < 0) {
      setenv (AVL_FILE_EMSG_VNAME, "20 open failed", 1);
      return (NULL);
   }
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);
   lim = lseek (fd, 0, SEEK_END);

   lseek (fd, 0, SEEK_SET);
   n = read (fd, &hdr, sizeof (hdr));
   if (n == 0) {
      memset (&hdr, 0, sizeof (hdr));
      memcpy (hdr.magic, "AVL.MW  ", 8);
      hdr.n_keys = n_keys;
      hdr.len = len;
      hdr.reclen = reclen;
      avl_dummy.fd = fd;
      avl_file_lwrite (&avl_dummy, &lim, 0, &hdr, sizeof (hdr));
   } else if (n != sizeof (hdr)) {
      setenv (AVL_FILE_EMSG_VNAME, "21 read header != sizeof (hdr)", 1);
      close (fd);
      return (NULL);
   }

   if (hdr.reclen != reclen) {
      setenv (AVL_FILE_EMSG_VNAME, "22 hdr.reclen != reclen", 1);
      close (fd);
      return (NULL);
   }

   if (hdr.n_keys != n_keys) {
      setenv (AVL_FILE_EMSG_VNAME, "23 hdr.n_keys != n_keys", 1);
      close (fd);
      return (NULL);
   }

   avl_fp = malloc (sizeof (AVL_FILE));
   if (avl_fp == NULL) { 
      setenv (AVL_FILE_EMSG_VNAME, "24 malloc returned NULL", 1);
      close (fd); 
      return (NULL);
   }
   avl_fp->fname = malloc (strlen (fname)+1);
   if (avl_fp->fname == NULL) {
      setenv (AVL_FILE_EMSG_VNAME, "25 malloc returned NULL", 1);
      close (fd);
      free (avl_fp);
      return (NULL);
   }
   strcpy (avl_fp->fname, fname);
   avl_fp->fd = fd;
   avl_fp->n_keys = n_keys;
   avl_fp->len = len;
   avl_fp->reclen = reclen;
   avl_fp->cmp = cmp;
#ifdef	AVL_FILE_TSAFE
   sem_init (&avl_fp->sem, 0, 1);
#endif

  /*
   * Search for an unused (unlocked) current-pointer record to
   * use first, or an empty record, before creating a new one.
   * The lock test does not detect locks by this process, so
   * check the PID.
   */
   pid = getpid ();
   for (cp = hdr.head_cpr; cp > 0; cp = cpr.next) {
      avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);

      if (sizeof (cpr.b) >= sizeof (pid_t)) {
         if (memcmp (&cpr.b, &pid, sizeof (pid_t)) != 0) {
            lseek (fd, cp, SEEK_SET);
            if (lockf (fd, F_TEST, reclen) == 0) break;
         }
      }
   }
   if (cp == 0) {
      cp = hdr.head_empty;
      if (cp == 0) {
         cp = lseek (fd, 0, SEEK_END);
      } else {
         avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);
         hdr.head_empty = cpr.next;
      }
      cpr.next = hdr.head_cpr;
      hdr.head_cpr = cp;
   }

   avl_fp->cpr = cp;

   for (i = 0; i < n_keys; i++) {
      cpr.n[i].b = 0x20; cpr.n[i].l = 0; cpr.n[i].r = 0;
   }
   if (sizeof (cpr.b) >= sizeof (pid_t)) memcpy (&cpr.b, &pid, sizeof (pid_t));
   cpr.prev = 0;
   avl_file_lwrite (avl_fp, &lim, cp, &cpr, reclen);
   lseek (fd, cp, SEEK_SET);
   lockf (fd, F_LOCK, reclen);

   avl_file_lwrite (avl_fp, &lim, 0, &hdr, sizeof (hdr));
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
   return (avl_fp);
}


//------------------------------------------- avl_file_close
void 
#ifdef	AVL_FILE_TSAFE
avl_file_close_t (AVL_FILE *avl_fp) 
#else
avl_file_close (AVL_FILE *avl_fp) 
#endif
{
   int32_t fd, reclen, i;

   struct hdr_struct {
      char magic[8];
      int32_t n_keys;
      int32_t len;        // AVL record data length
      int32_t reclen;     // AVL record length including key nodes
      int64_t n_avl;      // number of AVL records 
      int64_t nextnum;    // unique record numbers 
      off_t root[avl_fp->n_keys];
      off_t head_seq;     // doubly linked
      off_t head_empty;   // singly linked
      off_t head_cpr;     // singly linked
   } hdr;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;   // prev used as sequential pointer in 'cpr' records
      char b[avl_fp->len];
   } cpr, spr;
   off_t cp, sp, lim;


   reclen = avl_fp->reclen;
   fd = avl_fp->fd;

#ifdef AVL_FILE_TSAFE
   sem_wait (&avl_fp->sem);
#endif
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);
   lim = lseek (fd, 0, SEEK_END);

   avl_file_lread (avl_fp, &lim, 0, &hdr, sizeof (hdr));
   cp = avl_fp->cpr;
   avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);

   lseek (fd, cp, SEEK_SET);
   lockf (fd, F_ULOCK, reclen);

   if (hdr.head_cpr == cp) {
      hdr.head_cpr = cpr.next;
   } else {
      for (sp = hdr.head_cpr; sp > 0; sp = spr.next) {
         avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
         if (spr.next == cp) {
            spr.next = cpr.next;
            avl_file_lwrite (avl_fp, &lim, sp, &spr, reclen);
            break;
         }
      }
   }
   for (i = 0; i < avl_fp->n_keys; i++) {
      cpr.n[i].b = 0x40; cpr.n[i].l = 0; cpr.n[i].r = 0;
   }
   cpr.next = hdr.head_empty;
   hdr.head_empty = cp;

   avl_file_lwrite (avl_fp, &lim, cp, &cpr, reclen);
   avl_file_lwrite (avl_fp, &lim, 0, &hdr, sizeof (hdr));

   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
   close (fd);
#ifdef AVL_FILE_TSAFE
   sem_destroy (&avl_fp->sem);
#endif
   free (avl_fp->fname);
   free (avl_fp);
}


/*------------------------------------------- avl_file_getnum
 * Return a unique (sequential) record number. (Not the same as a
 * file array index, however, because records can be deleted,
 * for example.)
 */
int64_t 
#ifdef	AVL_FILE_TSAFE
avl_file_getnum_t (AVL_FILE *avl_fp) 
#else
avl_file_getnum (AVL_FILE *avl_fp) 
#endif
{
   int32_t fd;

   struct hdr_struct {
      char magic[8];
      int32_t n_keys; 
      int32_t len;        // AVL record data length
      int32_t reclen;     // AVL record length including key nodes
      int64_t n_avl;      // number of AVL records 
      int64_t nextnum;    // unique record numbers 
      off_t root[avl_fp->n_keys];
      off_t head_seq;     // doubly linked
      off_t head_empty;   // singly linked
      off_t head_cpr;     // singly linked
   } hdr;


   fd = avl_fp->fd;

#ifdef	AVL_FILE_TSAFE
   sem_wait (&avl_fp->sem);
#endif
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);

   lseek (fd, 0, SEEK_SET);
   read (fd, &hdr, sizeof (hdr));
   hdr.nextnum++;
   lseek (fd, 0, SEEK_SET);
   write (fd, &hdr, sizeof (hdr));

   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
#ifdef	AVL_FILE_TSAFE
   sem_post (&avl_fp->sem);
#endif
   return (hdr.nextnum);
}



/*------------------------------------------- avl_file_startseq
 * Initialize the sequential-access file pointer.
 */
void 
#ifdef AVL_FILE_TSAFE
avl_file_startseq_t (AVL_FILE *avl_fp) 
#else
avl_file_startseq (AVL_FILE *avl_fp) 
#endif
{
   int32_t fd, reclen;

   struct hdr_struct {
      char magic[8];
      int32_t n_keys; 
      int32_t len;        // AVL record data length
      int32_t reclen;     // AVL record length including key nodes
      int64_t n_avl;      // number of AVL records 
      int64_t nextnum;    // unique record numbers 
      off_t root[avl_fp->n_keys];
      off_t head_seq;     // doubly linked
      off_t head_empty;   // singly linked
      off_t head_cpr;     // singly linked
   } hdr;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;   // prev used as sequential pointer in 'cpr' records
      char b[avl_fp->len];
   } cpr;
   off_t cp, lim;


   reclen = avl_fp->reclen;
   fd = avl_fp->fd;
   cp = avl_fp->cpr;

#ifdef	AVL_FILE_TSAFE
   sem_wait (&avl_fp->sem);
#endif
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);
   lim = lseek (fd, 0, SEEK_END);

   lseek (fd, 0, SEEK_SET);
   read (fd, &hdr, sizeof (hdr));

   avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);
   cpr.prev = hdr.head_seq;
   avl_file_lwrite (avl_fp, &lim, cp, &cpr, reclen);

   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
#ifdef	AVL_FILE_TSAFE
   sem_post (&avl_fp->sem);
#endif
}



/*------------------------------------------- avl_file_readseq
 * Read the next sequential (unordered) file record, writing it
 * into the data buffer.
 * The return value is 0 for OK, or -1 for none.
 */
int32_t 
#ifdef	AVL_FILE_TSAFE
avl_file_readseq_t (AVL_FILE *avl_fp, void *data) 
#else
avl_file_readseq (AVL_FILE *avl_fp, void *data) 
#endif
{
   int32_t fd, reclen, ret;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;
      char b[avl_fp->len];
   } cpr, ar;
   off_t cp, lim;


   reclen = avl_fp->reclen;
   fd = avl_fp->fd;
   cp = avl_fp->cpr;

#ifdef	AVL_FILE_TSAFE
   sem_wait (&avl_fp->sem);
#endif
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);
   lim = lseek (fd, 0, SEEK_END);

   avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);

   if (cpr.prev == 0) {
      ret = -1;
   } else {
      avl_file_lread (avl_fp, &lim, cpr.prev, &ar, reclen);
      memcpy (data, ar.b, avl_fp->len);

      cpr.prev = ar.next;
      avl_file_lwrite (avl_fp, &lim, cp, &cpr, reclen);
      ret = 0;
   }

   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
#ifdef	AVL_FILE_TSAFE
   sem_post (&avl_fp->sem);
#endif
   return (ret);
}


/* ----------------------------------------------- avl_file_insert
 * Insert a new record, pointed to by the data parameter, into 
 * the AVL file. It returns 0 for success, or -1 for failure.
 *
 * (The internal format uses node elements .l and .r for left and 
 * right pointers, and negative values for previous and next threaded
 * retrieval. Also .b is the node balance.) This is taken mostly 
 * from "Fundamentals of Data Structures in Pascal" by Horowitz &
 * Sahni.
 */
int32_t 
#ifdef	AVL_FILE_TSAFE
avl_file_insert_t (AVL_FILE *avl_fp, void *data) 
#else
avl_file_insert (AVL_FILE *avl_fp, void *data) 
#endif
{
   int32_t fd, reclen, ret;

   struct hdr_struct {
      char magic[8];
      int32_t n_keys; 
      int32_t len;
      int32_t reclen;
      int64_t n_avl;
      int64_t nextnum;
      off_t root[avl_fp->n_keys];
      off_t head_seq;
      off_t head_empty;
      off_t head_cpr; 
   } hdr;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;
      char b[avl_fp->len];
   } yr, ar, br, cr, fr, pr, qr;
   off_t y, a, b, c, f, p, q, lim;
   int32_t k, d, unbalanced;


   reclen = avl_fp->reclen;
   fd = avl_fp->fd;
   ret = 0;

#ifdef	AVL_FILE_TSAFE
   sem_wait (&avl_fp->sem);
#endif
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);
   lim = lseek (fd, 0, SEEK_END);

   avl_file_lread (avl_fp, &lim, 0, &hdr, sizeof (hdr));


   if ((hdr.n_avl + 1) < 0) {
      setenv (AVL_FILE_EMSG_VNAME, "30 n_avl limit reached", 1);
      ret = -1;
      goto af_insert_return;
   }

   y = hdr.head_empty;
   if (y == 0) {
      y = lseek (fd, 0, SEEK_END);
      if (y < 0) {
         setenv (AVL_FILE_EMSG_VNAME, "31 lseek failed", 1);
         ret = -1;
         goto af_insert_return;
      }
   } else {
      avl_file_lread (avl_fp, &lim, y, &yr, reclen);
      hdr.head_empty = yr.next;
   }
   yr.prev = 0;
   yr.next = hdr.head_seq;
   if (yr.next > 0) {
      p = yr.next;
      avl_file_lread (avl_fp, &lim, p, &pr, reclen);
      pr.prev = y;
      avl_file_lwrite (avl_fp, &lim, p, &pr, reclen);
   }
   hdr.head_seq = y;

   memcpy (yr.b, data, avl_fp->len);
   avl_file_lwrite (avl_fp, &lim, y, &yr, reclen);

   for (k = 0; k < avl_fp->n_keys; k++) {
      avl_file_lread (avl_fp, &lim, y, &yr, reclen);

      a = hdr.root[k];
      if (a > 0) {
         avl_file_lread (avl_fp, &lim, a, &ar, reclen);
         f = 0; p = a; q = 0;
         while (p > 0) {
            avl_file_lread (avl_fp, &lim, p, &pr, reclen);
            if (pr.n[k].b != 0) {
               a = p; ar = pr; f = q; fr = qr;
            }
            if (avl_fp->cmp (k, yr.b, pr.b) < 0) {
               q = p; qr = pr; p = pr.n[k].l;
            } else {
               q = p; qr = pr; p = pr.n[k].r;
            }
         }
         if (avl_fp->cmp (k, yr.b, qr.b) < 0) {
            yr.n[k].b = 0; yr.n[k].l = p; yr.n[k].r = -q;
            qr.n[k].l = y;
         } else {
            yr.n[k].b = 0; yr.n[k].l = -q; yr.n[k].r = p;
            qr.n[k].r = y;
         }
         avl_file_lwrite (avl_fp, &lim, y, &yr, reclen);
         avl_file_lwrite (avl_fp, &lim, q, &qr, reclen);

         avl_file_lread (avl_fp, &lim, a, &ar, reclen);
         if (avl_fp->cmp (k, yr.b, ar.b) < 0) {
            p = ar.n[k].l; b = p; d = +1;
         } else {
            p = ar.n[k].r; b = p; d = -1;
         }
         while (p != y) {
            avl_file_lread (avl_fp, &lim, p, &pr, reclen);
            if (avl_fp->cmp (k, yr.b, pr.b) < 0) {
               pr.n[k].b = +1;
               avl_file_lwrite (avl_fp, &lim, p, &pr, reclen);
               p = pr.n[k].l;
            } else {
               pr.n[k].b = -1;
               avl_file_lwrite (avl_fp, &lim, p, &pr, reclen);
               p = pr.n[k].r;
            }
         }
         unbalanced = 1;
         if (ar.n[k].b == 0) {
            ar.n[k].b = d; unbalanced = 0;
            avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
         }
         if ((ar.n[k].b + d) == 0) {
            ar.n[k].b = 0; unbalanced = 0;
            avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
         }
         if (unbalanced == 1) {
            if (d == +1) {
               avl_file_lread (avl_fp, &lim, b, &br, reclen);
               if (br.n[k].b == +1) {
                  if (br.n[k].r > 0) 
                     ar.n[k].l = br.n[k].r;
                  else
                     ar.n[k].l = -b;
                  br.n[k].r = a; ar.n[k].b = 0; br.n[k].b = 0;
                  avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
                  avl_file_lwrite (avl_fp, &lim, b, &br, reclen);
               } else {
                  c = br.n[k].r;
                  avl_file_lread (avl_fp, &lim, c, &cr, reclen);
                  if (cr.n[k].l > 0) 
                     br.n[k].r = cr.n[k].l;
                  else
                     br.n[k].r = -c;
                  if (cr.n[k].r > 0) 
                     ar.n[k].l = cr.n[k].r;
                  else
                     ar.n[k].l = -c;
                  cr.n[k].l = b;
                  cr.n[k].r = a;
                  switch (cr.n[k].b) {
                  case +1:
                     ar.n[k].b = -1; br.n[k].b = 0; break;
                  case -1:
                     br.n[k].b = +1; ar.n[k].b = 0; break;
                  case 0:
                     br.n[k].b =  0; ar.n[k].b = 0; break;
                  default:
                     setenv (AVL_FILE_EMSG_VNAME, "32 invalid value n.b", 1);
                     break;
                  }
                  cr.n[k].b = 0;
                  avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
                  avl_file_lwrite (avl_fp, &lim, b, &br, reclen);
                  avl_file_lwrite (avl_fp, &lim, c, &cr, reclen);
                  b = c;
               }
            } else {
               avl_file_lread (avl_fp, &lim, b, &br, reclen);
               if (br.n[k].b == -1) {
                  if (br.n[k].l > 0) 
                     ar.n[k].r = br.n[k].l;
                  else
                     ar.n[k].r = -b;
                  br.n[k].l = a; ar.n[k].b = 0; br.n[k].b = 0;
                  avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
                  avl_file_lwrite (avl_fp, &lim, b, &br, reclen);
               } else {
                  c = br.n[k].l;
                  avl_file_lread (avl_fp, &lim, c, &cr, reclen);
                  if (cr.n[k].l > 0) 
                     ar.n[k].r = cr.n[k].l;
                  else
                     ar.n[k].r = -c;
                  if (cr.n[k].r > 0) 
                     br.n[k].l = cr.n[k].r;
                  else
                     br.n[k].l = -c;
                  cr.n[k].r = b;
                  cr.n[k].l = a;
                  switch (cr.n[k].b) {
                  case +1: 
                     br.n[k].b = -1; ar.n[k].b = 0; break;
                  case -1:
                     ar.n[k].b = +1; br.n[k].b = 0; break;
                  case 0:
                     br.n[k].b =  0; ar.n[k].b = 0; break;
                  default:
                     setenv (AVL_FILE_EMSG_VNAME, "33 invalid value n.b", 1);
                     break;
                  }
                  cr.n[k].b = 0;
                  avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
                  avl_file_lwrite (avl_fp, &lim, b, &br, reclen);
                  avl_file_lwrite (avl_fp, &lim, c, &cr, reclen);
                  b = c;
               }
            }
            if (f == 0) {
               hdr.root[k] = b;
            } else {
               if (a == fr.n[k].l) {
                  fr.n[k].l = b;
               } else if (a == fr.n[k].r) {
                  fr.n[k].r = b;
               }
               avl_file_lwrite (avl_fp, &lim, f, &fr, reclen);
            }
         }
      } else {
         yr.n[k].b = 0; yr.n[k].l = 0; yr.n[k].r = 0;
         hdr.root[k] = y;
         avl_file_lwrite (avl_fp, &lim, y, &yr, reclen);
      }
   }

   hdr.n_avl++;
   avl_file_lwrite (avl_fp, &lim, 0, &hdr, sizeof (hdr));

af_insert_return:
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
#ifdef	AVL_FILE_TSAFE
   sem_post (&avl_fp->sem);
#endif
   return (ret);
}
  


/* --------------------------------------------------- avl_file_delete
 * Delete one record from the file. The entire buffer pointed to by
 * the data parameter must match exactly the record to be deleted.
 * (i.e., it should be read first). If the file contains more than
 * one identical matching record, then the one deleted is arbitrary.
 *
 * The return value is 0 for OK, or -1 for none.
 */
int32_t 
#ifdef	AVL_FILE_TSAFE
avl_file_delete_t (AVL_FILE *avl_fp, void *data) 
#else
avl_file_delete (AVL_FILE *avl_fp, void *data) 
#endif
{
   int32_t fd, reclen, len, ret;

   struct hdr_struct {
      char magic[8];
      int32_t n_keys; 
      int32_t len;
      int32_t reclen;
      int64_t n_avl;
      int64_t nextnum;
      off_t root[avl_fp->n_keys];
      off_t head_seq;
      off_t head_empty;
      off_t head_cpr; 
   } hdr;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;
      char b[avl_fp->len];
   } yr, ar, br, cr, cpr, spr, ur, par[128];
   off_t y, a, b, c, cp, sp, pa[128], lim;
   int32_t i, k, l, m, updated, stack[128];


   fd = avl_fp->fd;
   reclen = avl_fp->reclen;
   len = avl_fp->len;
   ret = 0;

#ifdef	AVL_FILE_TSAFE
   sem_wait (&avl_fp->sem);
#endif
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);
   lim = lseek (fd, 0, SEEK_END);

   avl_file_lread (avl_fp, &lim, 0, &hdr, sizeof (hdr));

   memcpy (yr.b, data, len);
   y = 0;

  /*
   * Search for a matching record by key(s), assigning it to 'y'.
   * This may not find the matching record if there
   * are duplicate keys.
   */
   for (k = 0; k < avl_fp->n_keys; k++) {
      a = hdr.root[k];
      while (a > 0) {
         avl_file_lread (avl_fp, &lim, a, &ar, reclen);
         if (avl_fp->cmp (k, yr.b, ar.b) <= 0) {
            if (ar.n[k].l > 0)
               a = ar.n[k].l;
            else
               break;
         } else {
            if (ar.n[k].r > 0)
               a = ar.n[k].r;
            else {
               a = -ar.n[k].r;
               break;
            }
         }
      }
      if (a > 0) {
         avl_file_lread (avl_fp, &lim, a, &ar, reclen);
         if (avl_fp->cmp (k, yr.b, ar.b) == 0) {
            if (memcmp (yr.b, ar.b, len) == 0) {
               y = a; yr = ar;
               break;
            }
         }
      }
   }

  /*
   * Search for the record if not previously found, including
   * searching sequentially through duplicate keys.
   */
   if ((y == 0) && (avl_fp->n_keys > 0)) {
      k = 0; l = 0; m = 0;
      pa[l] = hdr.root[k];
af_delete_loop1:
      if (pa[l] > 0) {
         avl_file_lread (avl_fp, &lim, pa[l], &par[l], reclen);

         i = avl_fp->cmp (k, yr.b, par[l].b);
         if (i <= 0) {
            if (i == 0) stack[m++] = l;

            pa[l+1] = par[l].n[k].l; l++; 
            goto af_delete_loop1;
         }
af_delete_loop2:
         pa[l+1] = par[l].n[k].r; l++;
         goto af_delete_loop1;
      }
      if (m > 0) {
         l = stack[--m];
         for (i = 0; i < avl_fp->n_keys; i++) 
            if (avl_fp->cmp (i, yr.b, par[l].b) != 0) break;
         if ((i < avl_fp->n_keys) || (memcmp (yr.b, par[l].b, len) != 0))
            goto af_delete_loop2;         
         y = pa[l]; yr = par[l];
      }
   }


  /*
   * Search sequentially for a matching record, if necessary.
   * (This is needed, for example, if n_keys is zero).
   */
   if (y == 0) {
      a = hdr.head_seq;
      while (a > 0) {
         avl_file_lread (avl_fp, &lim, a, &ar, reclen);
         if (memcmp (yr.b, ar.b, len) == 0) {
            y = a; yr = ar;
            break;
         }
         a = ar.next;
      }
   }

   if (y == 0) {
      ret = -1;
      goto af_delete_return;
   } 

  /*
   * Find y's previous and next records for each key.
   */
   for (k = 0; k < avl_fp->n_keys; k++) {
      sp = yr.n[k].l;
      if (sp > 0) {
         avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
         while (spr.n[k].r > 0) {
            sp = spr.n[k].r;
            avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
         }
      } else {
         sp = -yr.n[k].l;
      }
      ur.n[k].l = sp;

      sp = yr.n[k].r;
      if (sp > 0) {
         avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
         while (spr.n[k].l > 0) {
            sp = spr.n[k].l;
            avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
         }
      } else {
         sp = -yr.n[k].r;
      }
      ur.n[k].r = sp;
   }

  /*
   * Advance all current pointers that point to this record.
   */
   cp = hdr.head_cpr;
   while (cp > 0) {
      avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);

      updated = 0;

      if (cpr.prev == y) {
         cpr.prev = yr.next;
         updated = 1;
      }

      for (k = 0; k < avl_fp->n_keys; k++) {
         if (cpr.n[k].l == y) {
            cpr.n[k].l = ur.n[k].l;
            updated = 1;
         }
         if (cpr.n[k].r == y) {
            cpr.n[k].r = ur.n[k].r;
            updated = 1;
         }
      }

      if (updated == 1) {
         avl_file_lwrite (avl_fp, &lim, cp, &cpr, reclen);
      }
      cp = cpr.next;
   }


  /*
   * Remove it from the tree. Only pointers change, not
   * the positions of records in the file.
   */
   for (k = 0; k < avl_fp->n_keys; k++) {
     /*
      * Make a path to y. Duplicate keys require some searching.
      */
      l = 0; m = 0;
      pa[l] = hdr.root[k];
afd_findloop1:
      if (pa[l] > 0) {
         avl_file_lread (avl_fp, &lim, pa[l], &par[l], reclen);

         i = avl_fp->cmp (k, yr.b, par[l].b);
         if (i <= 0) {
            if (i == 0) stack[m++] = l;

            pa[l+1] = par[l].n[k].l; l++; 
            goto afd_findloop1;
         }
afd_findloop2:
         pa[l+1] = par[l].n[k].r; l++;
         goto afd_findloop1;
      }
      if (m > 0) {
         l = stack[--m];
         if (pa[l] != y) goto afd_findloop2;         
      } else {
         setenv (AVL_FILE_EMSG_VNAME, "40 not in the tree", 1);
         continue;
      }
      m = l;

     /*
      * Remove and replace.
      */
      if (par[l].n[k].l > 0) {
         pa[l+1] = par[l].n[k].l; l++;
         avl_file_lread (avl_fp, &lim, pa[l], &par[l], reclen);

         if (par[l].n[k].r > 0) {
            while (par[l].n[k].r > 0) {
               pa[l+1] = par[l].n[k].r; l++;
               avl_file_lread (avl_fp, &lim, pa[l], &par[l], reclen);
            }

            if (par[l].n[k].l > 0) {
               par[l-1].n[k].r = par[l].n[k].l;
            } else {
               par[l-1].n[k].r = -pa[l];
            }
            par[l-1].n[k].b += 1;
            avl_file_lwrite (avl_fp, &lim, pa[l-1], &par[l-1], reclen);
         } else {
            yr.n[k].l = par[l].n[k].l;
            yr.n[k].b -= 1;
         }

         pa[m] = pa[l]; par[m] = par[l]; l--;
         par[m].n[k] = yr.n[k];
         avl_file_lwrite (avl_fp, &lim, pa[m], &par[m], reclen);

         if (yr.n[k].r > 0) {
            sp = ur.n[k].r;
            avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
            spr.n[k].l = -pa[m];
            avl_file_lwrite (avl_fp, &lim, sp, &spr, reclen);
         }

         if (m == 0) {
            hdr.root[k] = pa[m];
         } else {
            if (par[m-1].n[k].l == y) 
               par[m-1].n[k].l = pa[m];
            else
               par[m-1].n[k].r = pa[m];
            avl_file_lwrite (avl_fp, &lim, pa[m-1], &par[m-1], reclen);
         }

      } else if (par[l].n[k].r > 0) {
         pa[l+1] = par[l].n[k].r; l++;
         avl_file_lread (avl_fp, &lim, pa[l], &par[l], reclen);

         if (par[l].n[k].l > 0) {
            while (par[l].n[k].l > 0) {
               pa[l+1] = par[l].n[k].l; l++;
               avl_file_lread (avl_fp, &lim, pa[l], &par[l], reclen);
            }

            if (par[l].n[k].r > 0) {
               par[l-1].n[k].l = par[l].n[k].r;
            } else {
               par[l-1].n[k].l = -pa[l];
            }
            par[l-1].n[k].b -= 1;
            avl_file_lwrite (avl_fp, &lim, pa[l-1], &par[l-1], reclen);
         } else {
            yr.n[k].r = par[l].n[k].r;
            yr.n[k].b += 1;
         }

         pa[m] = pa[l]; par[m] = par[l]; l--;
         par[m].n[k] = yr.n[k];
         avl_file_lwrite (avl_fp, &lim, pa[m], &par[m], reclen);

         if (yr.n[k].l > 0) {
            sp = ur.n[k].l;
            avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
            spr.n[k].r = -pa[m];
            avl_file_lwrite (avl_fp, &lim, sp, &spr, reclen);
         }

         if (m == 0) {
            hdr.root[k] = pa[m];
         } else {
            if (par[m-1].n[k].l == y)
               par[m-1].n[k].l = pa[m]; 
            else
               par[m-1].n[k].r = pa[m]; 
            avl_file_lwrite (avl_fp, &lim, pa[m-1], &par[m-1], reclen);
         }

      } else {              // no sub-trees
         if (m == 0) {
            hdr.root[k] = 0;
         } else {
            if (par[m-1].n[k].l == y) {
               par[m-1].n[k].l = yr.n[k].l; 
               par[m-1].n[k].b -= 1;
            } else if (par[m-1].n[k].r == y) {
               par[m-1].n[k].r = yr.n[k].r; 
               par[m-1].n[k].b += 1;
            }
            avl_file_lwrite (avl_fp, &lim, pa[m-1], &par[m-1], reclen);
         }
         l--;
      }

     /*
      * Re-balance.
      */
      while (l >= 0) {
         a = pa[l]; ar = par[l];

        /*
         * 
         */
         if ((ar.n[k].b == +1) || (ar.n[k].b == -1)) break;

         if (ar.n[k].b == 0) {
            if (l > 0) {
               if (par[l-1].n[k].l == a) {
                  par[l-1].n[k].b -= 1;
               } else if (par[l-1].n[k].r == a) {
                  par[l-1].n[k].b += 1;
               }
               avl_file_lwrite (avl_fp, &lim, pa[l-1], &par[l-1], reclen);
            }
            l--;
            continue;
         }

        /*
         * Do a rotation around a. Do not decrement l afterwards.
         */
         if (ar.n[k].b == +2) {
            b = ar.n[k].l;
            avl_file_lread (avl_fp, &lim, b, &br, reclen);

            if ((br.n[k].b == 0) || (br.n[k].b == +1)) {
               if (br.n[k].r > 0) 
                  ar.n[k].l = br.n[k].r;
               else
                  ar.n[k].l = -b;
               br.n[k].r = a;
               if (br.n[k].b == 0) {
                  ar.n[k].b = +1; br.n[k].b = -1;
               } else {
                  ar.n[k].b =  0; br.n[k].b =  0;
               }
               avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
               avl_file_lwrite (avl_fp, &lim, b, &br, reclen);

               pa[l] = b; par[l] = br;
            } else {
               c = br.n[k].r;
               avl_file_lread (avl_fp, &lim, c, &cr, reclen);
               if (cr.n[k].l > 0) 
                  br.n[k].r = cr.n[k].l;
               else
                  br.n[k].r = -c;
               if (cr.n[k].r > 0) 
                  ar.n[k].l = cr.n[k].r;
               else
                  ar.n[k].l = -c;
               cr.n[k].l = b;
               cr.n[k].r = a;
               switch (cr.n[k].b) {
               case +1:
                  ar.n[k].b = -1; br.n[k].b = 0; break;
               case -1:
                  br.n[k].b = +1; ar.n[k].b = 0; break;
               case 0:
                  br.n[k].b =  0; ar.n[k].b = 0; break;
               default:
                  setenv (AVL_FILE_EMSG_VNAME, "41 invalid value n.b", 1);
                  break;
               }
               cr.n[k].b = 0;
               avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
               avl_file_lwrite (avl_fp, &lim, b, &br, reclen);
               avl_file_lwrite (avl_fp, &lim, c, &cr, reclen);

               pa[l] = c; par[l] = cr;
            }
         } else if (ar.n[k].b == -2) {
            b = ar.n[k].r; 
            avl_file_lread (avl_fp, &lim, b, &br, reclen);

            if ((br.n[k].b == 0) || (br.n[k].b == -1)) {
               if (br.n[k].l > 0) 
                  ar.n[k].r = br.n[k].l;
               else
                  ar.n[k].r = -b;
               br.n[k].l = a; 
               if (br.n[k].b == 0) {
                  ar.n[k].b = -1; br.n[k].b = +1;
               } else {
                  ar.n[k].b =  0; br.n[k].b =  0;
               }
               avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
               avl_file_lwrite (avl_fp, &lim, b, &br, reclen);

               pa[l] = b; par[l] = br;
            } else {
               c = br.n[k].l;
               avl_file_lread (avl_fp, &lim, c, &cr, reclen);
               if (cr.n[k].l > 0) 
                  ar.n[k].r = cr.n[k].l;
               else
                  ar.n[k].r = -c;
               if (cr.n[k].r > 0) 
                  br.n[k].l = cr.n[k].r;
               else
                  br.n[k].l = -c;
               cr.n[k].r = b;
               cr.n[k].l = a;
               switch (cr.n[k].b) {
               case +1: 
                  br.n[k].b = -1; ar.n[k].b = 0; break;
               case -1:
                  ar.n[k].b = +1; br.n[k].b = 0; break;
               case 0:
                  br.n[k].b =  0; ar.n[k].b = 0; break;
               default:
                  setenv (AVL_FILE_EMSG_VNAME, "42 invalid value n.b", 1);
                  break;
               }
               cr.n[k].b = 0;
               avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
               avl_file_lwrite (avl_fp, &lim, b, &br, reclen);
               avl_file_lwrite (avl_fp, &lim, c, &cr, reclen);

               pa[l] = c; par[l] = cr;
            }
         } else {
            setenv (AVL_FILE_EMSG_VNAME, "43 bad balance factor", 1);	// key  k
            break;
         }

         if (l == 0) {
            hdr.root[k] = pa[l];
         } else {
            if (par[l-1].n[k].l == a) {
               par[l-1].n[k].l = pa[l];
            } else if (par[l-1].n[k].r == a) {
               par[l-1].n[k].r = pa[l];
            }
            avl_file_lwrite (avl_fp, &lim, pa[l-1], &par[l-1], reclen);
         }
      } 
   }


  /*
   * Remove y from the sequential list.
   */
   if (yr.next > 0) {
      a = yr.next;
      avl_file_lread (avl_fp, &lim, a, &ar, reclen);
      ar.prev = yr.prev;
      avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
   }

   if (hdr.head_seq == y) {
      hdr.head_seq = yr.next;
   } else {
      a = yr.prev;
      avl_file_lread (avl_fp, &lim, a, &ar, reclen);
      ar.next = yr.next;
      avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
   }


  /*
   * Add it to the empty list.
   */
   yr.next = hdr.head_empty;
   hdr.head_empty = y;
   yr.prev = 0;
   for (i = 0; i < avl_fp->n_keys; i++) {
      yr.n[i].b = 0x40; yr.n[i].l = 0; yr.n[i].r = 0;
   }
   avl_file_lwrite (avl_fp, &lim, y, &yr, reclen);

   hdr.n_avl--;
   avl_file_lwrite (avl_fp, &lim, 0, &hdr, sizeof (hdr));

af_delete_return:
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
#ifdef	AVL_FILE_TSAFE
   sem_post (&avl_fp->sem);
#endif
   return (ret);
}


/* --------------------------------------------------- avl_file_update
 * Update a record with new information. For trees with multiple
 * keys, all of the keys must match for the update to succeed.
 * The return value is 0 for OK, or -1 for none.
 */
int32_t 
#ifdef	AVL_FILE_TSAFE
avl_file_update_t (AVL_FILE *avl_fp, void *data) 
#else
avl_file_update (AVL_FILE *avl_fp, void *data) 
#endif
{
   int32_t fd, reclen, len, ret;

   struct hdr_struct {
      char magic[8];
      int32_t n_keys; 
      int32_t len;
      int32_t reclen;
      int64_t n_avl;
      int64_t nextnum;
      off_t root[avl_fp->n_keys];
      off_t head_seq;
      off_t head_empty;
      off_t head_cpr; 
   } hdr;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;
      char b[avl_fp->len];
   } yr, par[128];
   off_t y, pa[128], lim;
   int32_t i, k, l, m, stack[128];


   fd = avl_fp->fd;
   reclen = avl_fp->reclen;
   len = avl_fp->len;

#ifdef	AVL_FILE_TSAFE
   sem_wait (&avl_fp->sem);
#endif
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);
   lim = lseek (fd, 0, SEEK_END);

   avl_file_lread (avl_fp, &lim, 0, &hdr, sizeof (hdr));

   memcpy (yr.b, data, len);
   y = 0;

  /*
   * Find the record. Duplicate keys require some searching.
   */
   if (avl_fp->n_keys > 0) {
      k = 0; l = 0; m = 0;
      pa[l] = hdr.root[k];
af_update_loop1:
      if (pa[l] > 0) {
         avl_file_lread (avl_fp, &lim, pa[l], &par[l], reclen);

         i = avl_fp->cmp (k, yr.b, par[l].b);
         if (i <= 0) {
            if (i == 0) stack[m++] = l;

            pa[l+1] = par[l].n[k].l; l++; 
            goto af_update_loop1;
         }
af_update_loop2:
         pa[l+1] = par[l].n[k].r; l++;
         goto af_update_loop1;
      }
      if (m > 0) {
         l = stack[--m];
         for (i = 0; i < avl_fp->n_keys; i++) 
            if (avl_fp->cmp (i, yr.b, par[l].b) != 0) break;
         if (i < avl_fp->n_keys) goto af_update_loop2;         
         y = pa[l]; yr = par[l];
      }
   }

   if (y == 0) {
      ret = -1;
   } else {
      memcpy (yr.b, data, len);
      avl_file_lwrite (avl_fp, &lim, y, &yr, reclen);
      ret = 0;
   }

   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
#ifdef	AVL_FILE_TSAFE
   sem_post (&avl_fp->sem);
#endif
   return (ret);
}



/*--------------------------------------------------- avl_file_startlt
 * Using key k, return the first record less than data. The data field
 * is over-written with the file record, if one exists.
 * The return value is 0 for OK, or -1 for none.
 */
int32_t
#ifdef	AVL_FILE_TSAFE
avl_file_startlt_t (AVL_FILE *avl_fp, void *data, int32_t k) 
#else
avl_file_startlt (AVL_FILE *avl_fp, void *data, int32_t k) 
#endif
{
   int32_t fd, reclen, ret;

   struct hdr_struct {
      char magic[8];
      int32_t n_keys; 
      int32_t len;
      int32_t reclen;
      int64_t n_avl;
      int64_t nextnum;
      off_t root[avl_fp->n_keys];
      off_t head_seq;
      off_t head_empty;
      off_t head_cpr; 
   } hdr;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;
      char b[avl_fp->len];
   } ar, br, cpr, sr;
   off_t a, cp, sp, lim;


   if ((k < 0) || (k >= avl_fp->n_keys)) {
      setenv (AVL_FILE_EMSG_VNAME, "70 the key index is out of bounds", 1);
      return (-1);
   }
   reclen = avl_fp->reclen;
   fd = avl_fp->fd;
   cp = avl_fp->cpr;
   ret = 0;

#ifdef	AVL_FILE_TSAFE
   sem_wait (&avl_fp->sem);
#endif
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);
   lim = lseek (fd, 0, SEEK_END);

   memcpy (br.b, data, avl_fp->len);

   avl_file_lread (avl_fp, &lim, 0, &hdr, sizeof (hdr));
   a = hdr.root[k];
   while (a > 0) {
      avl_file_lread (avl_fp, &lim, a, &ar, reclen);
      if (avl_fp->cmp (k, br.b, ar.b) <= 0) {
         if (ar.n[k].l > 0)
            a = ar.n[k].l;
         else {
            a = -ar.n[k].l;
            break;
         }
      } else {
         if (ar.n[k].r > 0)
            a = ar.n[k].r;
         else
            break;
      }
   }

   avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);

   if (a > 0) {
      avl_file_lread (avl_fp, &lim, a, &ar, reclen);
      memcpy (data, ar.b, avl_fp->len);

      sp = ar.n[k].l; 
      if (sp > 0) {
         avl_file_lread (avl_fp, &lim, sp, &sr, reclen);
         while (sr.n[k].r > 0) {
            sp = sr.n[k].r;
            avl_file_lread (avl_fp, &lim, sp, &sr, reclen);
         }
      } else {
         sp = -ar.n[k].l;
      }
      cpr.n[k].l = sp;

      sp = ar.n[k].r; 
      if (sp > 0) {
         avl_file_lread (avl_fp, &lim, sp, &sr, reclen);
         while (sr.n[k].l > 0) {
            sp = sr.n[k].l;
            avl_file_lread (avl_fp, &lim, sp, &sr, reclen);
         }
      } else {
         sp = -ar.n[k].r;
      }
      cpr.n[k].r = sp;
   } else {
      cpr.n[k].l = 0;
      cpr.n[k].r = 0;
      ret = -1;
   }

   avl_file_lwrite (avl_fp, &lim, cp, &cpr, reclen);

   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
#ifdef	AVL_FILE_TSAFE
   sem_post (&avl_fp->sem);
#endif
   return (ret);
}


/*--------------------------------------------------- avl_file_startge
 * Using key k, return the first record greater than or equal to data.
 * The data field is over-written with the file record, if one exists.
 * The return value is 0 for OK, or -1 for none.
 */
int32_t 
#ifdef	AVL_FILE_TSAFE
avl_file_startge_t (AVL_FILE *avl_fp, void *data, int32_t k) 
#else
avl_file_startge (AVL_FILE *avl_fp, void *data, int32_t k) 
#endif
{
   int32_t fd, reclen, ret;

   struct hdr_struct {
      char magic[8];
      int32_t n_keys; 
      int32_t len;      // AVL record data length
      int32_t reclen;   // AVL record length including key nodes
      int64_t n_avl;    // number of AVL records 
      int64_t nextnum;  // unique record numbers 
      off_t root[avl_fp->n_keys];
      off_t head_seq;
      off_t head_empty;
      off_t head_cpr; 
   } hdr;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;
      char b[avl_fp->len];
   } ar, br, cpr, sr;
   off_t a, cp, sp, lim;


   if ((k < 0) || (k >= avl_fp->n_keys)) {
      setenv (AVL_FILE_EMSG_VNAME, "80 the key index is out of bounds", 1);
      return (-1);
   }
   reclen = avl_fp->reclen;
   fd = avl_fp->fd;
   cp = avl_fp->cpr;
   ret = 0;

#ifdef	AVL_FILE_TSAFE
   sem_wait (&avl_fp->sem);
#endif
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);
   lim = lseek (fd, 0, SEEK_END);

   memcpy (br.b, data, avl_fp->len);

   avl_file_lread (avl_fp, &lim, 0, &hdr, sizeof (hdr));
   a = hdr.root[k];
   while (a > 0) {
      avl_file_lread (avl_fp, &lim, a, &ar, reclen);
      if (avl_fp->cmp (k, br.b, ar.b) <= 0) {
         if (ar.n[k].l > 0)
            a = ar.n[k].l;
         else
            break;
      } else {
         if (ar.n[k].r > 0)
            a = ar.n[k].r;
         else {
            a = -ar.n[k].r;
            break;
         }
      }
   }

   avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);

   if (a > 0) {
      avl_file_lread (avl_fp, &lim, a, &ar, reclen);
      memcpy (data, ar.b, avl_fp->len);

      sp = ar.n[k].l; 
      if (sp > 0) {
         avl_file_lread (avl_fp, &lim, sp, &sr, reclen);
         while (sr.n[k].r > 0) {
            sp = sr.n[k].r;
            avl_file_lread (avl_fp, &lim, sp, &sr, reclen);
         }
      } else {
         sp = -ar.n[k].l;
      }
      cpr.n[k].l = sp;

      sp = ar.n[k].r; 
      if (sp > 0) {
         avl_file_lread (avl_fp, &lim, sp, &sr, reclen);
         while (sr.n[k].l > 0) {
            sp = sr.n[k].l;
            avl_file_lread (avl_fp, &lim, sp, &sr, reclen);
         }
      } else {
         sp = -ar.n[k].r;
      }
      cpr.n[k].r = sp;
   } else {
      cpr.n[k].l = 0;
      cpr.n[k].r = 0;
      ret = -1;
   }

   avl_file_lwrite (avl_fp, &lim, cp, &cpr, reclen);

   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
#ifdef	AVL_FILE_TSAFE
   sem_post (&avl_fp->sem);
#endif
   return (ret);
}


/*--------------------------------------------------- avl_file_next
 * Using key k, read the next record into data buffer. 
 * Separate pointers are maintained for the 'previous' and 'next' functions. 
 * The return value is 0 for OK, or -1 for none.
 */
int32_t 
#ifdef	AVL_FILE_TSAFE
avl_file_next_t (AVL_FILE *avl_fp, void *data, int32_t k) 
#else
avl_file_next (AVL_FILE *avl_fp, void *data, int32_t k) 
#endif
{
   int32_t fd, reclen, ret;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;
      char b[avl_fp->len];
   } ar, cpr, sr;
   off_t a, cp, sp, lim;


   if ((k < 0) || (k >= avl_fp->n_keys)) {
      setenv (AVL_FILE_EMSG_VNAME, "90 the key index is out of bounds", 1);
      return (-1);
   }
   reclen = avl_fp->reclen;
   fd = avl_fp->fd;
   cp = avl_fp->cpr;
   ret = 0;

#ifdef	AVL_FILE_TSAFE
   sem_wait (&avl_fp->sem);
#endif
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);
   lim = lseek (fd, 0, SEEK_END);

   avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);

   a = cpr.n[k].r;
   if (a > 0) {
      avl_file_lread (avl_fp, &lim, a, &ar, reclen);
      memcpy (data, ar.b, avl_fp->len);

      sp = ar.n[k].r; 
      if (sp > 0) {
         avl_file_lread (avl_fp, &lim, sp, &sr, reclen);
         while (sr.n[k].l > 0) {
            sp = sr.n[k].l;
            avl_file_lread (avl_fp, &lim, sp, &sr, reclen);
         }
      } else {
         sp = -ar.n[k].r;
      }
      cpr.n[k].r = sp;

      avl_file_lwrite (avl_fp, &lim, cp, &cpr, reclen);
   } else 
      ret = -1;

   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
#ifdef	AVL_FILE_TSAFE
   sem_post (&avl_fp->sem);
#endif
   return (ret);
}


/*--------------------------------------------------- avl_file_prev
 * Using key k, read the previous record into the data buffer.
 * Separate pointers are maintained for the 'previous' and 'next' functions. 
 * The return value is 0 for OK, or -1 for none.
 */
int32_t 
#ifdef	AVL_FILE_TSAFE
avl_file_prev_t (AVL_FILE *avl_fp, void *data, int32_t k) 
#else
avl_file_prev (AVL_FILE *avl_fp, void *data, int32_t k) 
#endif
{
   int32_t fd, reclen, ret;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;
      char b[avl_fp->len];
   } ar, cpr, sr;
   off_t a, cp, sp, lim;


   if ((k < 0) || (k >= avl_fp->n_keys)) {
      setenv (AVL_FILE_EMSG_VNAME, "100 the key index is out of bounds", 1);
      return (-1);
   }
   reclen = avl_fp->reclen;
   fd = avl_fp->fd;
   cp = avl_fp->cpr;
   ret = 0;

#ifdef	AVL_FILE_TSAFE
   sem_wait (&avl_fp->sem);
#endif
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);
   lim = lseek (fd, 0, SEEK_END);

   avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);

   a = cpr.n[k].l;
   if (a > 0) {
      avl_file_lread (avl_fp, &lim, a, &ar, reclen);
      memcpy (data, ar.b, avl_fp->len);

      sp = ar.n[k].l;
      if (sp > 0) {
         avl_file_lread (avl_fp, &lim, sp, &sr, reclen);
         while (sr.n[k].r > 0) {
            sp = sr.n[k].r;
            avl_file_lread (avl_fp, &lim, sp, &sr, reclen);
         }
      } else {
         sp = -ar.n[k].l;
      }
      cpr.n[k].l = sp;

      avl_file_lwrite (avl_fp, &lim, cp, &cpr, reclen);
   } else 
      ret = -1;

   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
#ifdef	AVL_FILE_TSAFE
   sem_post (&avl_fp->sem);
#endif
   return (ret);
}


/*--------------------------------------------------- avl_file_find
 * Find a record using key k, over-writing the data buffer with
 * the matching file record, if one exists.
 * The return value is 0 for OK, or -1 for none.
 */
int32_t
#ifdef	AVL_FILE_TSAFE
avl_file_find_t (AVL_FILE *avl_fp, void *data, int32_t k) 
#else
avl_file_find (AVL_FILE *avl_fp, void *data, int32_t k) 
#endif
{
   char b[avl_fp->len];

   memcpy (b, data, avl_fp->len);
#ifdef	AVL_FILE_TSAFE
   if (avl_file_startge_t (avl_fp, b, k) == 0) { 
#else
   if (avl_file_startge (avl_fp, b, k) == 0) { 
#endif
      if (avl_fp->cmp (k, b, data) == 0) {
         memcpy (data, b, avl_fp->len);
         return (0);
      }
   }
   return (-1);
}


/*------------------------------------------------- avl_file_scan
 * Recursively scan a tree by the order of key k. The variable sp and
 * 'count' must be zero initially when calling this function.
 * The return value is the height of the tree. The variable 
 * 'count' contains the record count.
 */
int32_t 
#ifdef	AVL_FILE_TSAFE
avl_file_scan_t (AVL_FILE *avl_fp, int32_t k, off_t sp, int64_t *count) 
#else
avl_file_scan (AVL_FILE *avl_fp, int32_t k, off_t sp, int64_t *count) 
#endif
{
   int32_t fd, reclen;

   struct hdr_struct {
      char magic[8];
      int32_t n_keys; 
      int32_t len;      // AVL record data length
      int32_t reclen;   // AVL record length including key nodes
      int64_t n_avl;    // number of AVL records 
      int64_t nextnum;  // unique record numbers 
      off_t root[avl_fp->n_keys];
      off_t head_seq;
      off_t head_empty;
      off_t head_cpr; 
   } hdr;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;
      char b[avl_fp->len];
   } sr;
   off_t lim;
   int32_t hl, hr, h;


   if ((k < 0) || (k >= avl_fp->n_keys)) {
      setenv (AVL_FILE_EMSG_VNAME, "110 the key index is out of bounds", 1);
      return (-1);
   }
   reclen = avl_fp->reclen;
   fd = avl_fp->fd;


   if (sp == 0) {
#ifdef	AVL_FILE_TSAFE
      sem_wait (&avl_fp->sem);
#endif
      lseek (fd, 0, SEEK_SET);
      lockf (fd, F_LOCK, 1);

      lseek (fd, 0, SEEK_SET);
      read (fd, &hdr, sizeof (hdr));

      if (hdr.root[k] > 0) {
#ifdef	AVL_FILE_TSAFE
         h = avl_file_scan_t (avl_fp, k, hdr.root[k], count);
#else
         h = avl_file_scan (avl_fp, k, hdr.root[k], count);
#endif
      } else {
         h = 0;
      }

      lseek (fd, 0, SEEK_SET);
      lockf (fd, F_ULOCK, 1);
#ifdef	AVL_FILE_TSAFE
      sem_post (&avl_fp->sem);
#endif
      if (*count != hdr.n_avl) {
         setenv (AVL_FILE_EMSG_VNAME, "50 count != hdr.n_avl", 1);
//       fprintf (stderr, "avl_file_scan: count %lld != %lld\n", *count, hdr.n_avl);
      }
   } else if (sp > 0) {
      lim = lseek (fd, 0, SEEK_END);
      avl_file_lread (avl_fp, &lim, sp, &sr, reclen);

     *count += 1;
      hl = 1; hr = 1;
#ifdef	AVL_FILE_TSAFE
      if (sr.n[k].l > 0) hl += avl_file_scan_t (avl_fp, k, sr.n[k].l, count);
      if (sr.n[k].r > 0) hr += avl_file_scan_t (avl_fp, k, sr.n[k].r, count);
#else
      if (sr.n[k].l > 0) hl += avl_file_scan (avl_fp, k, sr.n[k].l, count);
      if (sr.n[k].r > 0) hr += avl_file_scan (avl_fp, k, sr.n[k].r, count);
#endif
      if (sr.n[k].b != hl - hr) {
         setenv (AVL_FILE_EMSG_VNAME, "51 bad balance", 1);	// key k
//       fprintf (stderr, "avl_file_scan: key %d bad balance = %2d\n", k, sr.n[k].b);
      }

      h = (hl > hr) ? hl : hr;
   } else {
      h = 0;
   }
   return (h);
}


/*------------------------------------------- avl_file_lock
 * Place an advisory lock on the file at lseek position 1.
 * Note that lseek position 0 is already used by the other
 * functions.
 * 
 * This function does not block process threads from concurrent access,
 * because file locks do not guarantee exclusive access for threads.
 */
#ifdef	AVL_FILE_TSAFE

void 
avl_file_lock_t (AVL_FILE *avl_fp) 
{
   sem_wait (&avl_fp->sem);
   lseek (avl_fp->fd, 1, SEEK_SET);
   lockf (avl_fp->fd, F_LOCK, 1);
   sem_post (&avl_fp->sem);
}

#else

void 
avl_file_lock (AVL_FILE *avl_fp) 
{
   lseek (avl_fp->fd, 1, SEEK_SET);
   lockf (avl_fp->fd, F_LOCK, 1);
}

#endif


/*------------------------------------------- avl_file_unlock
 * Remove the lock at lseek position 1 from the file.
 */
#ifdef	AVL_FILE_TSAFE

void 
avl_file_unlock_t (AVL_FILE *avl_fp) 
{
   sem_wait (&avl_fp->sem);
   lseek (avl_fp->fd, 1, SEEK_SET);
   lockf (avl_fp->fd, F_ULOCK, 1);
   sem_post (&avl_fp->sem);
}

#else

void 
avl_file_unlock (AVL_FILE *avl_fp) 
{
   lseek (avl_fp->fd, 1, SEEK_SET);
   lockf (avl_fp->fd, F_ULOCK, 1);
}

#endif



/*------------------------------------------- avl_file_dump
 * Show record nodes for debugging.
 */
void 
#ifdef	AVL_FILE_TSAFE
avl_file_dump_t (AVL_FILE *avl_fp) 
#else
avl_file_dump (AVL_FILE *avl_fp) 
#endif
{
   int32_t fd, n, i, reclen;

   struct hdr_struct {
      char magic[8];
      int32_t n_keys; 
      int32_t len;        // AVL record data length
      int32_t reclen;     // AVL record length including key nodes
      int64_t n_avl;      // number of AVL records 
      int64_t nextnum;    // unique record numbers 
      off_t root[avl_fp->n_keys];
      off_t head_seq;     // doubly linked
      off_t head_empty;   // singly linked
      off_t head_cpr;     // singly linked
   } hdr;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;   // prev used as sequential pointer in 'cpr' records
      char b[avl_fp->len];
   } pr;                 // per-process current position pointer


   fd = avl_fp->fd;
   reclen = avl_fp->reclen;

   lseek (fd, 0, SEEK_SET);
   read (fd, &hdr, sizeof (hdr));

   printf ("hdr: n_keys %d, len %d, reclen %d, n_avl %lld, head_seq %d, head_empty %d, head_cpr %d\n",
           hdr.n_keys, hdr.len, hdr.reclen, hdr.n_avl,
           (int) hdr.head_seq, (int) hdr.head_empty, (int) hdr.head_cpr);

   printf ("hdr: ");
   for (i = 0; i < avl_fp->n_keys; i++) {
      printf ("%2d: %6d | ", i, (int) hdr.root[i]);
   }
   printf ("\n");


   for (;;) {
      printf ("  pos %6ld: ", lseek (fd, 0, SEEK_CUR)); 
      n = read (fd, &pr, reclen);
      if (n != reclen) {
         printf ("\n");
         break;
      }
      for (i = 0; i < avl_fp->n_keys; i++) {
         printf ("%2d:%3d %6ld %6ld | ", i, pr.n[i].b, pr.n[i].l, pr.n[i].r);
      }
      printf (" prev %6ld, next %6ld | ", pr.prev, pr.next);
//    printf (" (%s)", &pr.b[0]);	// show data?
      printf ("\n");
   }
}



/* --------------------------------------------------- avl_file_squash
 * Move empty records to the end of the file and shorten it.
 * Shortening is limited to the last 'current-pointer' record
 * for files opened more than once.
 */
void 
#ifdef	AVL_FILE_TSAFE
avl_file_squash_t (AVL_FILE *avl_fp) 
#else
avl_file_squash (AVL_FILE *avl_fp) 
#endif
{
   int32_t fd, reclen, len;

   struct hdr_struct {
      char magic[8];
      int32_t n_keys; 
      int32_t len;        // AVL record data length
      int32_t reclen;     // AVL record length including key nodes
      int64_t n_avl;      // number of AVL records 
      int64_t nextnum;    // unique record numbers 
      off_t root[avl_fp->n_keys];
      off_t head_seq;     // doubly linked
      off_t head_empty;   // singly linked
      off_t head_cpr;     // singly linked
   } hdr;

   struct avl_struct {
      struct avl_node_struct n[avl_fp->n_keys];
      off_t prev, next;   // prev used as sequential pointer in 'cpr' records
      char b[avl_fp->len];
   } cpr, spr, ar, br, yr, zr, pr, qr, par[128];
   off_t cp, sp, a, b, y, z, p, q, pa[128], lim;
   int32_t i, k, l, m, updated, stack[128];
   pid_t pid;


   fd = avl_fp->fd;
   reclen = avl_fp->reclen;
   len = avl_fp->len;

#ifdef	AVL_FILE_TSAFE
   sem_wait (&avl_fp->sem);
#endif
   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_LOCK, 1);
   lim = lseek (fd, 0, SEEK_END);

   avl_file_lread (avl_fp, &lim, 0, &hdr, sizeof (hdr));

  /*
   * Search for unused (unlocked) current-pointer records to
   * remove and add to the empty list.
   */
   pid = getpid ();
   sp = 0;
   for (cp = hdr.head_cpr; cp > 0; cp = cpr.next) {
      avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);

      if (sizeof (cpr.b) >= sizeof (pid_t)) {
         if (memcmp (&cpr.b, &pid, sizeof (pid_t)) != 0) {
            lseek (fd, cp, SEEK_SET);
            if (lockf (fd, F_TEST, reclen) == 0) {
               if (sp > 0) {
                  avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
                  spr.next = cpr.next;
                  avl_file_lwrite (avl_fp, &lim, sp, &spr, reclen);
               } else {
                  hdr.head_cpr = cpr.next;
               }
               a = cp; ar = cpr;
               for (i = 0; i < avl_fp->n_keys; i++) {
                  ar.n[i].b = 0x40; ar.n[i].l = 0; ar.n[i].r = 0;
               }
               ar.next = hdr.head_empty;
               hdr.head_empty = a;
               avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
               continue;
            }
         }
      }

      sp = cp;
   }

  /*
   * Loop eliminating the empty records.
   */
   while (hdr.head_empty > 0) {
      avl_file_lwrite (avl_fp, &lim, 0, &hdr, sizeof (hdr));

      a = 0; b = hdr.head_empty;
      p = 0; q = hdr.head_empty;     

      y = 0;
      for (sp = hdr.head_empty; sp > 0; sp = spr.next) {
         avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
         if (sp <= b) { b = sp; br = spr; a = y; ar = yr; }
         if (sp >= q) { q = sp; qr = spr; p = y; pr = yr; }
         y = sp; yr = spr;
      }

      y = lim - reclen;
      avl_file_lread (avl_fp, &lim, y, &yr, reclen);

     /*
      * Is the last record an empty record?
      */
      if (y == q) {
         if (p > 0) {
            pr.next = qr.next;
            avl_file_lwrite (avl_fp, &lim, p, &pr, reclen);
         } else {
            hdr.head_empty = qr.next;
         }
         lim = y;
         i = ftruncate (fd, lim);
         if (i != 0) {
            setenv (AVL_FILE_EMSG_VNAME, "60 ftruncate failed", 1);
            break;
         }
         continue;
      }

     /*
      * Is the last record the cpr record?
      */
      if (y == avl_fp->cpr) {
         lseek (fd, y, SEEK_SET);
         lockf (fd, F_ULOCK, reclen);

         if (hdr.head_cpr == y) {
            hdr.head_cpr = yr.next;
         } else {
            for (sp = hdr.head_cpr; sp > 0; sp = spr.next) {
               avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
               if (spr.next == y) {
                  spr.next = yr.next;
                  avl_file_lwrite (avl_fp, &lim, sp, &spr, reclen);
                  break;
               }
            }
         }

         if (a > 0) {
            ar.next = br.next;
            avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
         } else {
            hdr.head_empty = br.next;
         }

         avl_fp->cpr = b;
         br = yr;
         br.next = hdr.head_cpr;
         hdr.head_cpr = b;
         avl_file_lwrite (avl_fp, &lim, b, &br, reclen);

         lseek (fd, b, SEEK_SET);
         lockf (fd, F_LOCK, reclen);

         lim = y;
         i = ftruncate (fd, lim);
         if (i != 0) {
            setenv (AVL_FILE_EMSG_VNAME, "61 ftruncate failed", 1);
            break;
         }
         continue;
      }

     /*
      * Is the last record a tree node record?
      */
      if (avl_fp->n_keys == 0) {
         for (cp = hdr.head_cpr; cp > 0; cp = cpr.next) {
            avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);
            if (y == cp) break;
         }
         if (y == cp) break;
      } else if (abs (yr.n[0].b) > 1) {
         if (yr.n[0].b != 0x20) {	// 'current-pointer' record
            setenv (AVL_FILE_EMSG_VNAME, "62 unknown last record", 1);
         }
         break;
      }

     /*
      * The last record is a tree node, so move it to the first
      * empty record location.
      */
      if (a > 0) {
         ar.next = br.next;
         avl_file_lwrite (avl_fp, &lim, a, &ar, reclen);
      } else {
         hdr.head_empty = br.next;
      }
      br = yr;
      avl_file_lwrite (avl_fp, &lim, b, &br, reclen);

     /*
      * Take it off the sequential list.
      */
      if (yr.next > 0) {
         z = yr.next;
         avl_file_lread (avl_fp, &lim, z, &zr, reclen);
         if (zr.prev != y) {
            setenv (AVL_FILE_EMSG_VNAME, "63 bad sequential list pointer", 1);
            break;
         }
         zr.prev = b;
         avl_file_lwrite (avl_fp, &lim, z, &zr, reclen);
      }

      if (yr.prev > 0) {
         z = yr.prev;
         avl_file_lread (avl_fp, &lim, z, &zr, reclen);
         if (zr.next != y) {
            setenv (AVL_FILE_EMSG_VNAME, "64 bad sequential list pointer", 1);
            break;
         }
         zr.next = b;
         avl_file_lwrite (avl_fp, &lim, z, &zr, reclen);
      } else {
         hdr.head_seq = b;
      }

     /*
      * Find all tree node pointers to 'y' and change them to 'b'.
      */
      for (k = 0; k < avl_fp->n_keys; k++) {
        /*
         * Make a path to y. Duplicate keys require some searching.
         */
         l = 0; m = 0;
         pa[l] = hdr.root[k];
af_squash_loop1:
         if (pa[l] > 0) {
            avl_file_lread (avl_fp, &lim, pa[l], &par[l], reclen);

            i = avl_fp->cmp (k, yr.b, par[l].b);
            if (i <= 0) {
               if (i == 0) stack[m++] = l;

               pa[l+1] = par[l].n[k].l; l++; 
               goto af_squash_loop1;
            }
af_squash_loop2:
            pa[l+1] = par[l].n[k].r; l++;
            goto af_squash_loop1;
         }
         if (m > 0) {
            l = stack[--m];
            if (pa[l] != y) goto af_squash_loop2;         
         } else {
            setenv (AVL_FILE_EMSG_VNAME, "65 not in the tree", 1);	// key k
            continue;
         }
         m = l;
         if (l > 0) {
            if (par[l-1].n[k].l == y) 
               par[l-1].n[k].l = b;
            else
               par[l-1].n[k].r = b;
            avl_file_lwrite (avl_fp, &lim, pa[l-1], &par[l-1], reclen);
         } else {
            hdr.root[k] = b;
         }

        /*
         * Change thread pointers.
         */
         sp = yr.n[k].l;
         if (sp > 0) {
            avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
            while (spr.n[k].r > 0) {
               sp = spr.n[k].r;
               avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
            }
            spr.n[k].r = -b;
            avl_file_lwrite (avl_fp, &lim, sp, &spr, reclen);
         }

         sp = yr.n[k].r;
         if (sp > 0) {
            avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
            while (spr.n[k].l > 0) {
               sp = spr.n[k].l;
               avl_file_lread (avl_fp, &lim, sp, &spr, reclen);
            }
            spr.n[k].l = -b;
            avl_file_lwrite (avl_fp, &lim, sp, &spr, reclen);
         }
      }

     /*
      * Go through the cpr list changing 'y' pointers to 'b'.
      */
      for (cp = hdr.head_cpr; cp > 0; cp = cpr.next) {
         avl_file_lread (avl_fp, &lim, cp, &cpr, reclen);

         updated = 0;

         if (cpr.prev == y) {
            cpr.prev = b;
            updated = 1;
         }

         for (k = 0; k < avl_fp->n_keys; k++) {
            if (cpr.n[k].l == y) {
               cpr.n[k].l = b;
               updated = 1;
            }
            if (cpr.n[k].r == y) {
               cpr.n[k].r = b;
               updated = 1;
            }
         }

         if (updated == 1) {
            avl_file_lwrite (avl_fp, &lim, cp, &cpr, reclen);
         }
      }

      lim = y;
      i = ftruncate (fd, lim);
      if (i != 0) {
         setenv (AVL_FILE_EMSG_VNAME, "66 ftruncate failed", 1);
         break;
      }
   }

   avl_file_lwrite (avl_fp, &lim, 0, &hdr, sizeof (hdr));

   lseek (fd, 0, SEEK_SET);
   lockf (fd, F_ULOCK, 1);
#ifdef	AVL_FILE_TSAFE
   sem_post (&avl_fp->sem);
#endif
}
