#
# FIXME: Making the shared library gets the wrong version number,
#        and library not found errors, so is not used.
#
#lib_LTLIBRARIES = libavl_file.la
#libavl_file_la_SOURCES = avl_file.c avl_file.h

#
# Workaround: Create a static library only.
#
lib_LIBRARIES = libavl_file.a
libavl_file_a_SOURCES = avl_file.c avl_file.h

#
# TODO: I have not figured out how to specify to automake to compile the
#       same source twice with different C pre-processor flags, and put
#       the result into one single library. So for now, the thread-safe
#       versions of the functions are omitted from the library.
#
#libavl_file_la_CFLAGS = -pthread
#libavl_file_la_CPPFLAGS = -DAVL_FILE_TSAFE

include_HEADERS = avl_file.h

dist_man3_MANS = avl_file.3
#dist_info_TEXINFOS = avl_file.texi
