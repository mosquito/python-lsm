#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <structmember.h>
#include <string.h>
#include <assert.h>

#include "lz4/lib/lz4.h"
#include "zstd/lib/zstd.h"
#include "sqlite/ext/lsm1/lsm.h"

#define IS_64_BIT (sizeof(void*)==8)
#define LSM_MAX_AUTOFLUSH 1048576
#define PYLSM_DEFAULT_COMPRESS_LEVEL -65535

#define LZ4_COMP_LEVEL_DEFAULT 16
#define LZ4_COMP_LEVEL_MAX 16

typedef struct {
	PyObject_HEAD
	char         *path;
	lsm_db       *lsm;
	int          state;
	int          compressed;
	unsigned int compressor_id;
	int          autoflush;
	int          page_size;
	int          block_size;
	int          safety;
	int          autowork;
	int          mmap;
	int          use_log;
	int          automerge;
	int          max_freelist;
	int          multiple_processes;
	int          autocheckpoint;
	int          readonly;
	int 	     tx_level;
	int          compress_level;
	char		 binary;
	PyObject     *logger;
	lsm_compress lsm_compress;
	lsm_env      *lsm_env;
	lsm_mutex    *lsm_mutex;
	PyObject*	 weakrefs;
} LSM;


typedef struct {
	PyObject_HEAD
	uint8_t		state;
	lsm_cursor* cursor;
	LSM*        db;
	int 		seek_mode;
	PyObject*	weakrefs;
} LSMCursor;


typedef struct {
	PyObject_HEAD
	LSM *db;
	uint8_t	   state;
	lsm_cursor *cursor;
	PyObject*  weakrefs;
} LSMIterView;


typedef struct {
	PyObject_HEAD
	LSM *db;
	lsm_cursor *cursor;

	PyObject *start;
	char* pStart;
	Py_ssize_t nStart;

	PyObject *stop;
	char* pStop;
	Py_ssize_t nStop;

	int state;

	long step;
	char direction;

	Py_ssize_t counter;
	PyObject* weakrefs;
} LSMSliceView;


typedef struct {
	PyObject_HEAD
	LSM *db;
	int tx_level;
	int state;
	PyObject* weakrefs;
} LSMTransaction;


static PyTypeObject LSMType;
static PyTypeObject LSMCursorType;
static PyTypeObject LSMKeysType;
static PyTypeObject LSMValuesType;
static PyTypeObject LSMItemsType;
static PyTypeObject LSMSliceType;
static PyTypeObject LSMTransactionType;


static PyObject* LSMCursor_new(PyTypeObject*, LSM*, int);
static PyObject* LSMTransaction_new(PyTypeObject *type, LSM*);


enum {
	PY_LSM_INITIALIZED = 0,
	PY_LSM_OPENED = 1,
	PY_LSM_CLOSED = 2,
	PY_LSM_ITERATING = 3
};

enum {
	PY_LSM_SLICE_FORWARD = 0,
	PY_LSM_SLICE_BACKWARD = 1
};

enum {
	PY_LSM_COMPRESSOR_EMPTY = LSM_COMPRESSION_EMPTY,
	PY_LSM_COMPRESSOR_NONE = LSM_COMPRESSION_NONE,
	PY_LSM_COMPRESSOR_LZ4 = 1024,
	PY_LSM_COMPRESSOR_ZSTD = 2048,
};


static int pylsm_error(int rc) {
	switch (rc) {
		case LSM_OK:
			break;
		case LSM_ERROR:
			PyErr_SetString(PyExc_RuntimeError, "Error occurred");
			break;
		case LSM_BUSY:
			PyErr_SetString(PyExc_RuntimeError, "Busy");
			break;
		case LSM_NOMEM:
			PyErr_SetString(PyExc_MemoryError, "LSM memory error");
			break;
		case LSM_READONLY:
			PyErr_SetString(PyExc_PermissionError, "Read only");
			break;
		case LSM_IOERR:
			PyErr_SetString(PyExc_OSError, "IO error");
			break;
		case LSM_CORRUPT:
			PyErr_SetString(PyExc_RuntimeError, "Corrupted");
			break;
		case LSM_FULL:
			PyErr_SetString(PyExc_RuntimeError, "Full");
			break;
		case LSM_CANTOPEN:
			PyErr_SetString(PyExc_FileNotFoundError, "Can not open");
			break;
		case LSM_PROTOCOL:
			PyErr_SetString(PyExc_FileNotFoundError, "Protocol error");
			break;
		case LSM_MISUSE:
			PyErr_SetString(PyExc_RuntimeError, "Misuse");
			break;
		case LSM_MISMATCH:
			PyErr_SetString(PyExc_RuntimeError, "Mismatch");
			break;
		case LSM_IOERR_NOENT:
			PyErr_SetString(PyExc_SystemError, "NOENT");
			break;
		default:
			PyErr_Format(PyExc_RuntimeError, "Unhandled error: %d", rc);
			break;
	}

	return rc;
}


static int LSM_MutexLock(LSM* self) {
	self->lsm_env->xMutexEnter(self->lsm_mutex);
	return LSM_OK;
}


static int LSM_MutexLeave(LSM* self) {
	self->lsm_env->xMutexLeave(self->lsm_mutex);
	return LSM_OK;
}


static int pylsm_lz4_xBound(LSM* self, int nIn) {
	int rc = LZ4_compressBound(nIn);
	assert(rc > 0);
	return rc;
}


static int pylsm_lz4_xCompress(LSM* self, char *pOut, int *pnOut, const char *pIn, int nIn) {
	int acceleration = (2 << (15 - self->compress_level)) + 1;
	int rc = LZ4_compress_fast((const char*)pIn, pOut, nIn, *pnOut, acceleration);
	assert(rc > 0);
	*pnOut = rc;
	return LSM_OK;
}


static int pylsm_lz4_xUncompress(LSM* self, char *pOut, int *pnOut, const char *pIn, int nIn) {
	int rc = LZ4_decompress_safe((const char*)pIn, (char*)pOut, nIn, *pnOut);
	assert(rc > 0);
	*pnOut = rc;
	return LSM_OK;
}


static size_t pylsm_zstd_xBound(LSM* self, int nIn) {
	return ZSTD_compressBound(nIn);
}


static size_t pylsm_zstd_xCompress(LSM* self, char *pOut, Py_ssize_t *pnOut, const char *pIn, int nIn) {
	size_t rc = ZSTD_compress(pOut, *pnOut, pIn, nIn, self->compress_level);

	assert(!ZSTD_isError(rc));

	*pnOut = rc;
	return LSM_OK;
}


static int pylsm_zstd_xUncompress(LSM* self, char *pOut, Py_ssize_t *pnOut, const char *pIn, int nIn) {
  Py_ssize_t rc = ZSTD_decompress((char*)pOut, *pnOut, (const char*)pIn, nIn);
  assert(!ZSTD_isError(rc));
  *pnOut = rc;
  return 0;
}


static uint32_t is_power_of_two(uint32_t n) {
   if (n==0) return 0;
   return (ceil(log2(n)) == floor(log2(n)));
}


static void pylsm_logger(LSM* self, int rc, const char * message) {
	if (self->logger == NULL) return;

	PyGILState_STATE state = PyGILState_Ensure();
	PyObject_CallFunction(self->logger, "sI", message, rc);
	PyErr_Print();
	PyGILState_Release(state);
}


static int pylsm_seek_mode_check(int seek_mode) {
	switch (seek_mode) {
		case LSM_SEEK_EQ:
			return 0;
		case LSM_SEEK_LE:
			return 0;
		case LSM_SEEK_GE:
			return 0;
		case LSM_SEEK_LEFAST:
			return 0;
		default:
			PyErr_Format(
				PyExc_ValueError,
				"\"seek_mode\" should be one of SEEK_LEFAST (%d), SEEK_LE (%d), SEEK_EQ(%d) or SEEK_GE (%d) not %d",
				LSM_SEEK_LEFAST, LSM_SEEK_LE, LSM_SEEK_EQ, LSM_SEEK_GE, seek_mode
			);
			return -1;
	}
}


static Py_ssize_t pylsm_csr_length(lsm_cursor* cursor, Py_ssize_t *result) {
	Py_ssize_t counter = 0;
	int rc = 0;

	if ((rc = lsm_csr_first(cursor))) return rc;

	while (lsm_csr_valid(cursor)) {
		counter++;
		if ((rc = lsm_csr_next(cursor))) break;
	}

	*result = counter;
	return rc;
}


static Py_ssize_t pylsm_length(lsm_db* lsm, Py_ssize_t *result) {
	Py_ssize_t rc = 0;
	lsm_cursor *cursor;

	if ((rc = lsm_csr_open(lsm, &cursor))) return rc;
	rc = pylsm_csr_length(cursor, result);
	lsm_csr_close(cursor);
	return rc;
}


static int pylsm_getitem(
	lsm_db* lsm,
	const char * pKey,
	int nKey,
	char** ppVal,
	int* pnVal,
	int seek_mode
) {
	int rc;
	lsm_cursor *cursor;
	char* pValue = NULL;
	int nValue = 0;
	char* result = NULL;

	if ((rc = lsm_csr_open(lsm, &cursor))) return rc;
	if ((rc = lsm_csr_seek(cursor, pKey, nKey, seek_mode))) {
		lsm_csr_close(cursor);
		return rc;
	}
	if (!lsm_csr_valid(cursor)) {
		lsm_csr_close(cursor);
		return -1;
	}

	if (seek_mode == LSM_SEEK_LEFAST) {
		*pnVal = 0;
		lsm_csr_close(cursor);
		return rc;
	}

	if ((rc = lsm_csr_value(cursor, (const void **)&pValue, &nValue))) {
		lsm_csr_close(cursor);
		return rc;
	}

	result = calloc(nValue, sizeof(char));
	memcpy(result, pValue, nValue);
	lsm_csr_close(cursor);

	*ppVal = result;
	*pnVal = nValue;
	return 0;
}


static int pylsm_delitem(
	lsm_db* lsm,
	const char * pKey,
	int nKey
) {
	int rc = 0;
	lsm_cursor *cursor;

	if ((rc = lsm_csr_open(lsm, &cursor))) return rc;
	if ((rc = lsm_csr_seek(cursor, pKey, nKey, LSM_SEEK_EQ))) {
		lsm_csr_close(cursor);
		return rc;
	}
	if (!lsm_csr_valid(cursor)) {
		lsm_csr_close(cursor);
		return -1;
	}
	lsm_csr_close(cursor);
	if ((rc = lsm_delete(lsm, pKey, nKey))) return rc;
	return 0;
}


static int pylsm_contains(lsm_db* lsm, const char* pKey, int nKey) {
	int rc;
	lsm_cursor *cursor;

	if ((rc = lsm_csr_open(lsm, &cursor))) return rc;
	if ((rc = lsm_csr_seek(cursor, pKey, nKey, LSM_SEEK_EQ))) {
		lsm_csr_close(cursor);
		return rc;
	}

	if (!lsm_csr_valid(cursor)) { rc = -1; } else { rc = 0; }
	lsm_csr_close(cursor);
	return rc;
}


static int pylsm_ensure_opened(LSM* self) {
	if (self == NULL) {
		PyErr_SetString(PyExc_MemoryError, "Instance deallocated");
		return -1;
	}
	if (self->state == PY_LSM_OPENED) return 0;

	PyErr_SetString(PyExc_RuntimeError, "Database has not opened");
	return -1;
}

static int pylsm_ensure_writable(LSM* self) {
	if (pylsm_ensure_opened(self)) return -1;
	if (self->readonly) return pylsm_error(LSM_READONLY);
	return 0;
}

static int pylsm_ensure_csr_opened(LSMCursor* self) {
	if (pylsm_ensure_opened(self->db)) return 0;

	switch (self->state) {
		case PY_LSM_OPENED:
		case PY_LSM_ITERATING:
			if (!lsm_csr_valid(self->cursor)) {
				PyErr_SetString(PyExc_RuntimeError, "Invalid cursor");
				return -1;
			}
			return 0;
		default:
			PyErr_SetString(PyExc_RuntimeError, "Cursor closed");
			return -1;
	}
}


int pylsm_slice_first(LSMSliceView* self) {
	int rc;
	int cmp_res;

	if (self->pStop != NULL) {
		if ((rc = lsm_csr_cmp(self->cursor, self->pStop, (int) self->nStop, &cmp_res))) return rc;
		if (self->direction == PY_LSM_SLICE_FORWARD && cmp_res > 0) return -1;
		if (self->direction == PY_LSM_SLICE_BACKWARD && cmp_res < 0) return -1;
	}

	if (!lsm_csr_valid(self->cursor)) return -1;

	return 0;
}


int pylsm_slice_next(LSMSliceView* self) {
	int rc;
	int cmp_res = -65535;

	while (lsm_csr_valid(self->cursor)) {
		switch (self->direction) {
			case PY_LSM_SLICE_FORWARD:
				if ((rc = lsm_csr_next(self->cursor))) return rc;
				break;
			case PY_LSM_SLICE_BACKWARD:
				if ((rc = lsm_csr_prev(self->cursor))) return rc;
				break;
		}

		if (!lsm_csr_valid(self->cursor)) break;

		if (self->pStop != NULL) {
			if ((rc = lsm_csr_cmp(self->cursor, self->pStop, (int) self->nStop, &cmp_res))) return rc;
			if (self->direction == PY_LSM_SLICE_FORWARD && cmp_res > 0) break;
			if (self->direction == PY_LSM_SLICE_BACKWARD && cmp_res < 0) break;
		}

		self->counter++;
		if ((self->counter % self->step) == 0) return 0;
	}

	return -1;
}


static inline int pylsm_seek_mode_direction(int direction) {
	return (direction == PY_LSM_SLICE_FORWARD) ? LSM_SEEK_GE : LSM_SEEK_LE;
}


static int pylsm_slice_view_iter(LSMSliceView *self) {
	int rc;

	if ((rc = lsm_csr_open(self->db->lsm, &self->cursor))) return rc;

	int seek_mode = pylsm_seek_mode_direction(self->direction);

	if (self->pStart != NULL) {
		if ((rc = lsm_csr_seek(self->cursor, self->pStart, (int) self->nStart, seek_mode))) return rc;
	} else {
		switch (self->direction) {
			case PY_LSM_SLICE_FORWARD:
				if ((rc = lsm_csr_first(self->cursor))) return rc;
				break;
			case PY_LSM_SLICE_BACKWARD:
				if ((rc = lsm_csr_last(self->cursor))) return rc;
				break;
		}
	}

	return LSM_OK;
}


static int str_or_bytes_check(char binary, PyObject* pObj, const char** ppBuff, Py_ssize_t* nBuf) {
	const char * buff = NULL;
	Py_ssize_t buff_len = 0;

	if (binary) {
		if (PyBytes_Check(pObj)) {
			buff_len = PyBytes_GET_SIZE(pObj);
			buff = PyBytes_AS_STRING(pObj);
		} else {
			PyErr_Format(PyExc_ValueError, "bytes expected not %R", PyObject_Type(pObj));
			return -1;
		}
	} else {
		if (PyUnicode_Check(pObj)) {
			buff = PyUnicode_AsUTF8AndSize(pObj, &buff_len);
			if (buff == NULL) return -1;
		} else {
			PyErr_Format(PyExc_ValueError, "str expected not %R", PyObject_Type(pObj));
			return -1;
		}
	}

	*ppBuff = buff;
	*nBuf = buff_len;

	return 0;
}


static PyObject* pylsm_cursor_key_fetch(lsm_cursor* cursor, uint8_t binary) {
	char *pKey = NULL;
	int nKey = 0;
	char *pValue = NULL;
	int nValue = 0;

	if (pylsm_error(lsm_csr_key(cursor, (const void**) &pKey, &nKey))) return NULL;
	if (pylsm_error(lsm_csr_value(cursor, (const void**) &pValue, &nValue))) return NULL;

	if (binary) {
		return PyBytes_FromStringAndSize(pKey, nKey);
	} else {
		return PyUnicode_FromStringAndSize(pKey, nKey);
	}
}


static PyObject* pylsm_cursor_value_fetch(lsm_cursor* cursor, uint8_t binary) {
	char *pKey = NULL;
	int nKey = 0;
	char *pValue = NULL;
	int nValue = 0;

	if (pylsm_error(lsm_csr_key(cursor, (const void**) &pKey, &nKey))) return NULL;
	if (pylsm_error(lsm_csr_value(cursor, (const void**) &pValue, &nValue))) return NULL;

	if (binary) {
		return PyBytes_FromStringAndSize(pValue, nValue);
	} else {
		return PyUnicode_FromStringAndSize(pValue, nValue);
	}
}


static PyObject* pylsm_cursor_items_fetch(lsm_cursor* cursor, uint8_t binary) {
	char *pKey = NULL;
	int nKey = 0;
	char *pValue = NULL;
	int nValue = 0;

	lsm_csr_key(cursor, (const void**) &pKey, &nKey);
	lsm_csr_value(cursor, (const void**) &pValue, &nValue);

	PyObject* pyKey;
	PyObject* pyValue;

	if (binary) {
		pyKey = PyBytes_FromStringAndSize(pKey, nKey);
		pyValue = PyBytes_FromStringAndSize(pValue, nValue);
	} else {
		pyKey = PyUnicode_FromStringAndSize(pKey, nKey);
		pyValue = PyUnicode_FromStringAndSize(pValue, nValue);
	}

	PyObject* result = PyTuple_Pack(2, pyKey, pyValue);
	Py_DECREF(pyKey);
	Py_DECREF(pyValue);
	return result;
}


static PyObject* LSMIterView_new(PyTypeObject *type) {
	LSMIterView *self;
	self = (LSMIterView *) type->tp_alloc(type, 0);
	return (PyObject *) self;
}


static void LSMIterView_dealloc(LSMIterView *self) {
	if (self->db == NULL) return;

	if (self->cursor != NULL) {
		Py_BEGIN_ALLOW_THREADS
		LSM_MutexLock(self->db);
		lsm_csr_close(self->cursor);
		LSM_MutexLeave(self->db);
		Py_END_ALLOW_THREADS
	}

	if (self->state == PY_LSM_OPENED) {
		self->state = PY_LSM_CLOSED;
	}

	Py_DECREF(self->db);

	self->cursor = NULL;
	self->db = NULL;

	if (self->weakrefs != NULL) PyObject_ClearWeakRefs((PyObject *) self);
}


static int LSMIterView_init(LSMIterView *self, LSM* lsm) {
	if (pylsm_ensure_opened(lsm)) return -1;

	self->db = lsm;
	Py_INCREF(self->db);

	self->state = PY_LSM_INITIALIZED;
	return 0;
}


static Py_ssize_t LSMIterView_len(LSMIterView* self) {
	if (pylsm_ensure_opened(self->db)) return -1;

	Py_ssize_t result = 0;
	Py_ssize_t rc = 0;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self->db);
	rc = pylsm_length(self->db->lsm, &result);
	LSM_MutexLeave(self->db);
	Py_END_ALLOW_THREADS

	if (pylsm_error(rc)) return -1;
	return result;
}

static LSMIterView* LSMIterView_iter(LSMIterView* self) {
	if (pylsm_ensure_opened(self->db)) return NULL;

	if (self->state != PY_LSM_INITIALIZED) {
		Py_INCREF(self);
		return self;
	}

	if (self->state == PY_LSM_OPENED) {
		PyErr_SetString(PyExc_RuntimeError, "Can not modify started iterator");
		return NULL;
	}

	self->state = PY_LSM_OPENED;

	LSM_MutexLock(self->db);
	if (pylsm_error(lsm_csr_open(self->db->lsm, &self->cursor))) {
		LSM_MutexLeave(self->db);
	    return NULL;
	}

	if (pylsm_error(lsm_csr_first(self->cursor))) {
		LSM_MutexLeave(self->db);
		return NULL;
	}

	LSM_MutexLeave(self->db);

	Py_INCREF(self);
	return self;
}


static PyObject* LSMKeysView_next(LSMIterView *self) {
	if (pylsm_ensure_opened(self->db)) return NULL;
	if (self->state != PY_LSM_OPENED) {
		PyErr_SetString(PyExc_RuntimeError, "Must call __iter__ before __next__");
		return NULL;
	}

	if (!lsm_csr_valid(self->cursor)) {
		if (self->state != PY_LSM_CLOSED) {
			self->state = PY_LSM_CLOSED;
		}

		PyErr_SetNone(PyExc_StopIteration);
		return NULL;
	}

	LSM_MutexLock(self->db);
	PyObject* result = pylsm_cursor_key_fetch(self->cursor, self->db->binary);

	if (result == NULL) {
		LSM_MutexLeave(self->db);
		return NULL;
	}

	if (pylsm_error(lsm_csr_next(self->cursor))) {
		LSM_MutexLeave(self->db);
		return NULL;
	};

	LSM_MutexLeave(self->db);
	return result;
}


static PyObject* LSMValuesView_next(LSMIterView *self) {
	if (pylsm_ensure_opened(self->db)) return NULL;

	if (!lsm_csr_valid(self->cursor)) {
		if (self->state != PY_LSM_CLOSED) {
			self->state = PY_LSM_CLOSED;
		}
		PyErr_SetNone(PyExc_StopIteration);
		return NULL;
	}

	LSM_MutexLock(self->db);
	PyObject* result = pylsm_cursor_value_fetch(self->cursor, self->db->binary);
	if (result == NULL) {
		LSM_MutexLeave(self->db);
		return NULL;
	}

	if (pylsm_error(lsm_csr_next(self->cursor))) {
		LSM_MutexLeave(self->db);
		return NULL;
	};

	LSM_MutexLeave(self->db);

	return result;
}


static PyObject* LSMItemsView_next(LSMIterView *self) {
	if (pylsm_ensure_opened(self->db)) return NULL;

	if (!lsm_csr_valid(self->cursor)) {
		if (self->state != PY_LSM_CLOSED) {
			self->state = PY_LSM_CLOSED;
		}
		PyErr_SetNone(PyExc_StopIteration);
		return NULL;
	}

	LSM_MutexLock(self->db);
	PyObject* result = pylsm_cursor_items_fetch(
		self->cursor,
		self->db->binary
	);
	if (result == NULL) {
		LSM_MutexLeave(self->db);
		return NULL;
	}

	if (pylsm_error(lsm_csr_next(self->cursor))) {
		LSM_MutexLeave(self->db);
		return NULL;
	};

	LSM_MutexLeave(self->db);
	return result;
}


static int LSM_contains(LSM *self, PyObject *key);

static int LSMKeysView_contains(LSMIterView* self, PyObject* key) {
	return LSM_contains(self->db, key);
}

static PySequenceMethods LSMKeysView_sequence = {
	.sq_length = (lenfunc) LSMIterView_len,
	.sq_contains = (objobjproc) LSMKeysView_contains
};


static int LSMIterView_contains(LSMIterView* self, PyObject* key) {
	PyErr_SetNone(PyExc_NotImplementedError);
	return 0;
}


static PySequenceMethods LSMIterView_sequence = {
	.sq_length = (lenfunc) LSMIterView_len,
	.sq_contains = (objobjproc) LSMIterView_contains
};

static PyTypeObject LSMKeysType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "lsm_keys",
	.tp_basicsize = sizeof(LSMIterView),
	.tp_itemsize = 0,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_dealloc = (destructor) LSMIterView_dealloc,
	.tp_iter = (getiterfunc) LSMIterView_iter,
	.tp_iternext = (iternextfunc) LSMKeysView_next,
	.tp_as_sequence = &LSMKeysView_sequence,
	.tp_weaklistoffset = offsetof(LSMIterView, weakrefs)
};


static PyTypeObject LSMItemsType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "lsm_items",
	.tp_basicsize = sizeof(LSMIterView),
	.tp_itemsize = 0,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_dealloc = (destructor) LSMIterView_dealloc,
	.tp_iter = (getiterfunc) LSMIterView_iter,
	.tp_iternext = (iternextfunc) LSMItemsView_next,
	.tp_as_sequence = &LSMIterView_sequence,
	.tp_weaklistoffset = offsetof(LSMIterView, weakrefs)
};


static PyTypeObject LSMValuesType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "lsm_values",
	.tp_basicsize = sizeof(LSMIterView),
	.tp_itemsize = 0,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_dealloc = (destructor) LSMIterView_dealloc,
	.tp_iter = (getiterfunc) LSMIterView_iter,
	.tp_iternext = (iternextfunc) LSMValuesView_next,
	.tp_as_sequence = &LSMIterView_sequence,
	.tp_weaklistoffset = offsetof(LSMIterView, weakrefs)
};


static PyObject* LSMSliceView_new(PyTypeObject *type) {
	LSMSliceView *self;
	self = (LSMSliceView *) type->tp_alloc(type, 0);
	return (PyObject *) self;
}


static void LSMSliceView_dealloc(LSMSliceView *self) {
	if (self->db == NULL) return;

	if (self->cursor != NULL) {
		LSM_MutexLock(self->db);
		lsm_csr_close(self->cursor);
		LSM_MutexLeave(self->db);
	}

	if (self->start != NULL) Py_DECREF(self->start);
	if (self->stop != NULL) Py_DECREF(self->stop);

	Py_DECREF(self->db);

	self->cursor = NULL;
	self->db = NULL;
	self->pStart = NULL;
	self->pStop = NULL;
	self->stop = NULL;

	if (self->weakrefs != NULL) PyObject_ClearWeakRefs((PyObject *) self);
}


static int LSMSliceView_init(
	LSMSliceView *self,
	LSM* lsm,
	PyObject* start,
	PyObject* stop,
	PyObject* step
) {
	assert(lsm != NULL);
	if (pylsm_ensure_opened(lsm)) return -1;

	if (step == Py_None) {
		self->step = 1;
	} else {
		if (!PyLong_Check(step)) {
			PyErr_Format(
				PyExc_ValueError,
				"step must be int not %R",
				PyObject_Type(step)
			);
			return -1;
		}
		self->step = PyLong_AsLong(step);
	}

	self->direction = (self->step > 0) ? PY_LSM_SLICE_FORWARD : PY_LSM_SLICE_BACKWARD;

	self->db = lsm;

	switch (self->direction) {
		case PY_LSM_SLICE_FORWARD:
			self->stop = stop;
			self->start = start;
			break;
		case PY_LSM_SLICE_BACKWARD:
			self->stop = start;
			self->start = stop;
			break;
	}

	self->pStop = NULL;
	self->nStop = 0;
	self->counter = 0;

	if (self->stop != Py_None) {
		if (str_or_bytes_check(self->db->binary, self->stop, (const char **) &self->pStop, &self->nStop)) return -1;
		Py_INCREF(self->stop);
	}

	if (self->start != Py_None) {
		if (str_or_bytes_check(self->db->binary, self->start, (const char **) &self->pStart, &self->nStart)) return -1;
		Py_INCREF(self->start);
	}

	self->state = PY_LSM_INITIALIZED;
	Py_INCREF(self->db);
	return 0;
}


static LSMSliceView* LSMSliceView_iter(LSMSliceView* self) {
	if (pylsm_ensure_opened(self->db)) return NULL;


	if (self->state != PY_LSM_INITIALIZED) {
		Py_INCREF(self);
		return self;
	}

	if (self->state == PY_LSM_OPENED) {
		PyErr_SetString(PyExc_RuntimeError, "Can not modify started iterator");
		return NULL;
	}

	self->state = PY_LSM_OPENED;

	int err;
	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self->db);
	err = pylsm_slice_view_iter(self);
	LSM_MutexLeave(self->db);
	Py_END_ALLOW_THREADS

	if (pylsm_error(err)) return NULL;

	Py_INCREF(self);
	return self;
}


static PyObject* LSMSliceView_next(LSMSliceView *self) {
	if (pylsm_ensure_opened(self->db)) return NULL;

	switch (self->state) {
		case PY_LSM_OPENED:
			break;
		case PY_LSM_ITERATING:
			break;
		case PY_LSM_CLOSED:
			PyErr_SetNone(PyExc_StopIteration);
			return NULL;
		default:
			PyErr_SetString(PyExc_RuntimeError, "Must call __iter__ before __next__");
			return NULL;
	}

	if (!lsm_csr_valid(self->cursor)) {
		if (self->state != PY_LSM_CLOSED) {
			self->state = PY_LSM_CLOSED;
		}
		PyErr_SetNone(PyExc_StopIteration);
		return NULL;
	}

	int rc;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self->db);

	if (self->state == PY_LSM_OPENED) {
		self->state = PY_LSM_ITERATING;
		rc = pylsm_slice_first(self);
	} else {
		rc = pylsm_slice_next(self);
	}

	LSM_MutexLeave(self->db);
	Py_END_ALLOW_THREADS

	if (rc == -1) {
		self->state = PY_LSM_CLOSED;
		PyErr_SetNone(PyExc_StopIteration);
		return NULL;
	}

	if (pylsm_error(rc)) return NULL;

	if (!lsm_csr_valid(self->cursor)) {
		self->state = PY_LSM_CLOSED;
		PyErr_SetNone(PyExc_StopIteration);
		return NULL;
	}

	return pylsm_cursor_items_fetch(self->cursor, self->db->binary);
}


static PyTypeObject LSMSliceType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "lsm_slice",
	.tp_basicsize = sizeof(LSMSliceView),
	.tp_itemsize = 0,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_dealloc = (destructor) LSMSliceView_dealloc,
	.tp_iter = (getiterfunc) LSMSliceView_iter,
	.tp_iternext = (iternextfunc) LSMSliceView_next,
	.tp_weaklistoffset = offsetof(LSMSliceView, weakrefs)
};


static PyObject* LSM_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
	LSM *self;
	self = (LSM *) type->tp_alloc(type, 0);
	return (PyObject *) self;
}


static int _LSM_close(LSM* self) {
	int result;

	Py_BEGIN_ALLOW_THREADS;
	LSM_MutexLock(self);
	result = lsm_close(self->lsm);
	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS;

	if (result == LSM_OK) {
		self->state = PY_LSM_CLOSED;
		self->lsm = NULL;
		self->lsm_env = NULL;
		self->lsm_mutex = NULL;
	}

	return result;
}


static void LSM_dealloc(LSM *self) {
	if (self->state != PY_LSM_CLOSED && self->lsm != NULL) pylsm_error(_LSM_close(self));
	if (self->lsm_mutex != NULL) self->lsm_env->xMutexDel(self->lsm_mutex);
	if (self->logger != NULL) Py_DECREF(self->logger);
	if (self->path != NULL) PyMem_Free(self->path);
	if (self->weakrefs != NULL) PyObject_ClearWeakRefs((PyObject *) self);
}


static int LSM_init(LSM *self, PyObject *args, PyObject *kwds) {
	self->autocheckpoint = 2048;
	self->autoflush = 1024;
	self->automerge = 4;
	self->autowork = 1;
	self->mmap = 0;
	self->block_size = 1024;
	self->max_freelist = 24;
	self->multiple_processes = 1;
	self->page_size = 4 * 1024;
	self->readonly = 0;
	self->safety = LSM_SAFETY_NORMAL;
	self->use_log = 1;
	self->tx_level = 0;
	self->compressed = 0;
	self->logger = NULL;
	self->compress_level = PYLSM_DEFAULT_COMPRESS_LEVEL;
	self->path = NULL;
	self->binary = 1;
	memset(&self->lsm_compress, 0, sizeof(lsm_compress));

	static char* kwlist[] = {
		"path",
		"autoflush",
		"page_size",
		"safety",
		"block_size",
		"automerge",
		"max_freelist",
		"autocheckpoint",
		"autowork",
		"mmap",
		"use_log",
		"multiple_processes",
		"readonly",
		"binary",
		"logger",
		"compress",
		"compress_level",
		NULL
	};

	PyObject* compress = Py_None;
	int compressor_id = LSM_COMPRESSION_NONE;

	PyObject* pyPath;
	const char *path;
	Py_ssize_t path_len;

	if (!PyArg_ParseTupleAndKeywords(
		args, kwds, "O|iiIIIIIppppppOOi", kwlist,
		&pyPath,
		&self->autoflush,
		&self->page_size,
		&self->safety,
		&self->block_size,
		&self->automerge,
		&self->max_freelist,
		&self->autocheckpoint,
		&self->autowork,
		&self->mmap,
		&self->use_log,
		&self->multiple_processes,
		&self->readonly,
		&self->binary,
		&self->logger,
		&compress,
		&self->compress_level
	)) return -1;

	if (!PyUnicode_Check(pyPath)) pyPath = PyObject_Str(pyPath);

	path = PyUnicode_AsUTF8AndSize(pyPath, &path_len);
	if (path == NULL) return -1;

	self->path = PyMem_Calloc(sizeof(char), path_len + 1);
	memcpy(self->path, path, path_len);

	self->state = PY_LSM_INITIALIZED;

	if (self->autoflush > LSM_MAX_AUTOFLUSH) {
		PyErr_Format(
			PyExc_ValueError,
			"The maximum allowable value for autoflush parameter "
			"is 1048576 (1GB). Not %d", self->autoflush
		);
		return -1;
	}

	if (self->autoflush < 0) {
		PyErr_Format(
			PyExc_ValueError,
			"The minimum allowable value for autoflush parameter "
			"is 0. Not %d", self->autoflush
		);
		return -1;
	}

	if (self->autocheckpoint <= 0) {
		PyErr_SetString(
			PyExc_ValueError,
			"autocheckpoint is not able to be zero or lower"
		);
		return -1;
	}

	if (!(
		is_power_of_two(self->block_size) &&
		self->block_size >= 64 &&
		self->block_size < 65537
	)) {
		PyErr_Format(
			PyExc_ValueError,
			"block_size parameter must be power of two between "
			"64 and 65535. Not %d",
			self->block_size
		);
		return -1;
	}

	switch (self->safety) {
		case LSM_SAFETY_OFF:
			break;
		case LSM_SAFETY_NORMAL:
			break;
		case LSM_SAFETY_FULL:
			break;
		default:
			PyErr_Format(
				PyExc_ValueError,
				"safety parameter must be SAFETY_OFF SAFETY_NORMAL "
				"or SAFETY_FULL. Not %d", self->safety
			);
			return -1;
	}

	if (compress == Py_None) {
		compressor_id = PY_LSM_COMPRESSOR_EMPTY;
	} else if (!PyUnicode_Check(compress)) {
		PyErr_Format(PyExc_ValueError, "str expected not %R", PyObject_Type(compress));
		return -1;
	} else if (PyUnicode_CompareWithASCIIString(compress, "none") == 0) {
		compressor_id = PY_LSM_COMPRESSOR_NONE;
	} else if (PyUnicode_CompareWithASCIIString(compress, "lz4") == 0) {
		compressor_id = PY_LSM_COMPRESSOR_LZ4;

		if (self->compress_level == PYLSM_DEFAULT_COMPRESS_LEVEL) {
			self->compress_level = LZ4_COMP_LEVEL_DEFAULT;
		}

		if (self->compress_level > LZ4_COMP_LEVEL_MAX || self->compress_level < 1) {
			PyErr_Format(
				PyExc_ValueError,
				"compress_level for lz4 must be between 1 and %d",
				 LZ4_COMP_LEVEL_MAX
			);
			return -1;
		}
	} else if (PyUnicode_CompareWithASCIIString(compress, "zstd") == 0) {
		compressor_id = PY_LSM_COMPRESSOR_ZSTD;
		if (self->compress_level == PYLSM_DEFAULT_COMPRESS_LEVEL) {
			self->compress_level = ZSTD_CLEVEL_DEFAULT;
		}

		if (self->compress_level > ZSTD_maxCLevel() || self->compress_level < 1) {
			PyErr_Format(
				PyExc_ValueError,
				"compress_level for zstd must be between 1 and %d", ZSTD_maxCLevel()
			);
			return -1;
		}

	} else {
		PyErr_Format(
			PyExc_ValueError,
			"compressor argument must be one of \"none\" (or None) \"lz4\" or \"zstd\", but not %R",
			compress
		);
		return -1;
	}

	if (compressor_id > PY_LSM_COMPRESSOR_NONE) self->compressed = 1;

	if (self->logger != NULL && !PyCallable_Check(self->logger)) {
		PyErr_Format(PyExc_ValueError, "object %R is not callable", self->logger);
		return -1;
	}

	if (self->logger != NULL) Py_INCREF(self->logger);
	if (pylsm_error(lsm_new(NULL, &self->lsm))) return -1;

	self->lsm_env = lsm_get_env(self->lsm);

	if (pylsm_error(self->lsm_env->xMutexNew(self->lsm_env, &self->lsm_mutex))) return -1;

	if (self->logger != NULL) {
		lsm_config_log(self->lsm, (void (*)(void *, int, const char *)) pylsm_logger, self);
	} else {
		lsm_config_log(self->lsm, NULL, NULL);
	}

	if (self->lsm == NULL) {
		PyErr_SetString(PyExc_MemoryError, "Can not allocate memory");
		return -1;
	}

	// Only before lsm_open
	if (self->compressed) {
		self->lsm_compress.pCtx = self;
		self->lsm_compress.iId = compressor_id;

		switch (compressor_id) {
			case PY_LSM_COMPRESSOR_LZ4:
				self->lsm_compress.xCompress = (int (*)(void *, char *, int *, const char *, int)) pylsm_lz4_xCompress;
				self->lsm_compress.xUncompress = (int (*)(void *, char *, int *, const char *, int)) pylsm_lz4_xUncompress;
				self->lsm_compress.xBound = (int (*)(void *, int)) pylsm_lz4_xBound;
				self->lsm_compress.xFree = NULL;
				break;
			case PY_LSM_COMPRESSOR_ZSTD:
				self->lsm_compress.xCompress = (int (*)(void *, char *, int *, const char *, int)) pylsm_zstd_xCompress;
				self->lsm_compress.xUncompress = (int (*)(void *, char *, int *, const char *, int)) pylsm_zstd_xUncompress;
				self->lsm_compress.xBound = (int (*)(void *, int)) pylsm_zstd_xBound;
				self->lsm_compress.xFree = NULL;
				break;
		}

		if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_SET_COMPRESSION, &self->lsm_compress))) return -1;
	}

	if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_BLOCK_SIZE, &self->block_size))) return -1;
	if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_MULTIPLE_PROCESSES, &self->multiple_processes))) return -1;
	if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_PAGE_SIZE, &self->page_size))) return -1;
	if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_READONLY, &self->readonly))) return -1;

	// Not only before lsm_open
	if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_AUTOCHECKPOINT, &self->autocheckpoint))) return -1;
	if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_AUTOFLUSH, &self->autoflush))) return -1;
	if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_AUTOMERGE, &self->automerge))) return -1;
	if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_AUTOWORK, &self->autowork))) return -1;
	if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_MAX_FREELIST, &self->max_freelist))) return -1;
	if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_MMAP, &self->mmap))) return -1;
	if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_SAFETY, &self->safety))) return -1;
	if (pylsm_error(lsm_config(self->lsm, LSM_CONFIG_USE_LOG, &self->use_log))) return -1;

	if (PyErr_Occurred()) return -1;

	return 0;
}


static PyObject* LSM_open(LSM *self) {
	if (self->state == PY_LSM_OPENED) {
		PyErr_SetString(PyExc_RuntimeError, "Database already opened");
		return NULL;
	}

	if (self->state == PY_LSM_CLOSED) {
		PyErr_SetString(PyExc_RuntimeError, "Database closed");
		return NULL;
	}

	int result;
	result = lsm_open(self->lsm, self->path);

	if (pylsm_error(result)) return NULL;

	if (self->readonly == 0) {
		Py_BEGIN_ALLOW_THREADS
		result = lsm_flush(self->lsm);
		Py_END_ALLOW_THREADS

		if (pylsm_error(result)) return NULL;

		Py_BEGIN_ALLOW_THREADS
		result = lsm_work(self->lsm, self->automerge, self->page_size, NULL);
		Py_END_ALLOW_THREADS

		if (pylsm_error(result)) return NULL;
	}

	self->state = PY_LSM_OPENED;
	Py_RETURN_TRUE;
}

static PyObject* LSM_close(LSM *self) {
	if (self->state == PY_LSM_CLOSED) {
		PyErr_SetString(PyExc_RuntimeError, "Database already closed");
		return NULL;
	}

	if (pylsm_error(_LSM_close(self))) return NULL;
	Py_RETURN_TRUE;
}


static PyObject* LSM_info(LSM *self) {
	if (pylsm_ensure_opened(self)) return NULL;

	int nwrite_result = 0,
		nread_result = 0,
		checkpoint_size_result = 0;

	int nwrite = 0,
	 	nread = 0,
	 	checkpoint_size = 0,
		tree_size_old = 0,
		tree_size_current = 0,
		tree_size_result = 0;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);

	nread_result = lsm_info(
		self->lsm, LSM_INFO_NREAD, &nread
	);

	if (!self->readonly) nwrite_result = lsm_info(
		self->lsm, LSM_INFO_NWRITE, &nwrite
	);

	if (!self->readonly) checkpoint_size_result = lsm_info(
		self->lsm, LSM_INFO_CHECKPOINT_SIZE, &checkpoint_size
	);

	if (!self->readonly) tree_size_result = lsm_info(
		self->lsm, LSM_INFO_TREE_SIZE, &tree_size_old, &tree_size_current
	);

	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (pylsm_error(nread_result)) return NULL;
	if (self->readonly) return Py_BuildValue("{si}", "nread", nread);

	if (pylsm_error(nwrite_result)) return NULL;
	if (pylsm_error(checkpoint_size_result)) return NULL;
	if (pylsm_error(tree_size_result)) return NULL;

	return Py_BuildValue(
		"{sisisis{sisi}}",
		"nwrite", nwrite,
		"nread", nread,
		"checkpoint_size_result", checkpoint_size,
		"tree_size", "old", tree_size_old, "current", tree_size_current
	);
}


static PyObject* LSM_ctx_enter(LSM *self) {
	if (self->state == PY_LSM_OPENED) return (PyObject*) self;

	Py_INCREF(self);

	LSM_open(self);
	if (PyErr_Occurred()) return NULL;

	return (PyObject*) self;
}


static PyObject* LSM_commit_inner(LSM *self, int tx_level);
static PyObject* LSM_rollback_inner(LSM *self, int tx_level);


static PyObject* LSM_ctx_exit(LSM *self, PyObject* args) {
	if (self->state == PY_LSM_CLOSED) { Py_RETURN_NONE; };

	PyObject *exc_type, *exc_value, *exc_tb;
    if (!PyArg_ParseTuple(args, "OOO", &exc_type, &exc_value, &exc_tb)) return NULL;
	if (exc_type == Py_None) {
		if (self->tx_level > 0) LSM_commit_inner(self, 0);
	} else {
		if (self->tx_level > 0) LSM_rollback_inner(self, 0);
	}

	if (pylsm_error(_LSM_close(self))) return NULL;
	Py_RETURN_NONE;
}


static PyObject* LSM_work(LSM *self, PyObject *args, PyObject *kwds) {
	if (pylsm_ensure_writable(self)) return NULL;

	static char *kwlist[] = {"nmerge", "nkb", "complete", NULL};

	char complete = 1;
	int nmerge = self->automerge;
	int nkb = self->page_size;

	if (!PyArg_ParseTupleAndKeywords(
		args, kwds, "|IIp", kwlist, &nmerge, &nkb, &complete
	)) return NULL;

	int result;
	int total_written = 0;
	int written = 0;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);

	do {
		result = lsm_work(self->lsm, nmerge, nkb, &written);
		total_written += written;
		if (nmerge < self->automerge) nmerge++;
	} while (complete && written > 0);

	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (pylsm_error(result)) return NULL;
	return Py_BuildValue("i", total_written);
}


static PyObject* LSM_flush(LSM *self) {
	if (pylsm_ensure_writable(self)) return NULL;

	int rc;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);
	rc = lsm_flush(self->lsm);
	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (pylsm_error(rc)) return NULL;
	Py_RETURN_TRUE;
}

static PyObject* LSM_checkpoint(LSM *self) {
	if (pylsm_ensure_writable(self)) return NULL;

	int result;
	int bytes_written = 0;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);
	result = lsm_checkpoint(self->lsm, &bytes_written);
	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (pylsm_error(result)) return NULL;
	return Py_BuildValue("i", bytes_written);
}

static PyObject* LSM_cursor(LSM *self, PyObject *args, PyObject *kwds) {
	if (pylsm_ensure_opened(self)) return NULL;

	int seek_mode = LSM_SEEK_GE;
	static char *kwlist[] = {"seek_mode", NULL};

	if (!PyArg_ParseTupleAndKeywords(args, kwds, "|i", kwlist, &seek_mode)) return NULL;
	if (pylsm_seek_mode_check(seek_mode)) return NULL;

	LSMCursor* cursor = (LSMCursor*) LSMCursor_new(&LSMCursorType, self, seek_mode);
	if (cursor == NULL) return NULL;

	return (PyObject*) cursor;
}


static PyObject* LSM_insert(LSM *self, PyObject *args, PyObject *kwds) {
	if (pylsm_ensure_writable(self)) return NULL;

	static char *kwlist[] = {"key", "value", NULL};

	const char* pKey = NULL;
	Py_ssize_t nKey = 0;

	const char* pVal = NULL;
	Py_ssize_t nVal = 0;

	if (self->binary) {
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#y#", kwlist, &pKey, &nKey, &pVal, &nVal)) return NULL;
	} else {
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "s#s#", kwlist, &pKey, &nKey, &pVal, &nVal)) return NULL;
	}

	if (nKey >= INT_MAX) {
		PyErr_SetString(PyExc_OverflowError, "length of key is too large");
		return NULL;
	}
	if (nVal >= INT_MAX) {
		PyErr_SetString(PyExc_OverflowError, "length of value is too large");
		return NULL;
	}

	int result;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);
	result = lsm_insert(self->lsm, pKey, (int) nKey, pVal, (int) nVal);
	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (pylsm_error(result)) return NULL;
	Py_RETURN_NONE;
}


static PyObject* LSM_delete(LSM *self, PyObject *args, PyObject *kwds) {
	if (pylsm_ensure_writable(self)) return NULL;

	static char *kwlist[] = {"key", NULL};

	const char* pKey = NULL;
	Py_ssize_t nKey = 0;

	if (self->binary) {
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#", kwlist, &pKey, &nKey)) return NULL;
	} else {
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "s#", kwlist, &pKey, &nKey)) return NULL;
	}

	if (nKey >= INT_MAX) {
		PyErr_SetString(PyExc_OverflowError, "length of key is too large");
		return NULL;
	}

	int result;
	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);
	result = lsm_delete(self->lsm, pKey, (int) nKey);
	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (pylsm_error(result)) return NULL;
	Py_RETURN_NONE;
}


static PyObject* LSM_delete_range(LSM *self, PyObject *args, PyObject *kwds) {
	if (pylsm_ensure_writable(self)) return NULL;

	static char *kwlist[] = {"start", "end", NULL};

	const char* pStart = NULL;
	Py_ssize_t nStart = 0;

	const char* pEnd = NULL;
	Py_ssize_t nEnd = 0;

	if (self->binary) {
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "y#y#", kwlist, &pStart, &nStart, &pEnd, &nEnd)) return NULL;
	} else {
		if (!PyArg_ParseTupleAndKeywords(args, kwds, "s#s#", kwlist, &pStart, &nStart, &pEnd, &nEnd)) return NULL;
	}

	if (nStart >= INT_MAX) {
		PyErr_SetString(PyExc_OverflowError, "length of start is too large");
		return NULL;
	}
	if (nEnd >= INT_MAX) {
		PyErr_SetString(PyExc_OverflowError, "length of end is too large");
		return NULL;
	}

	int result;
	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);
	result = lsm_delete_range(self->lsm, pStart, (int) nStart, pEnd, (int) nEnd);
	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (pylsm_error(result)) return NULL;
	Py_RETURN_NONE;
}


static PyObject* LSM_begin(LSM *self) {
	if (pylsm_ensure_writable(self)) return NULL;
	if (self->tx_level < 0) self->tx_level = 0;

	int level = self->tx_level + 1;
	int result;

	Py_BEGIN_ALLOW_THREADS
	result = lsm_begin(self->lsm, level);
	Py_END_ALLOW_THREADS

	if (pylsm_error(result)) return NULL;

	self->tx_level = level;

	Py_RETURN_TRUE;
}


static PyObject* LSM_commit_inner(LSM *self, int tx_level) {
	if (pylsm_ensure_writable(self)) return NULL;
	if (tx_level < 0) tx_level = 0;

	int result;
	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);
	result = lsm_commit(self->lsm, tx_level);
	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (pylsm_error(result)) return NULL;
	Py_RETURN_TRUE;
}


static PyObject* LSM_rollback_inner(LSM *self, int tx_level) {
	if (pylsm_ensure_writable(self)) return NULL;
	if (tx_level < 0) tx_level = 0;

	int result;
	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);
	result = lsm_rollback(self->lsm, tx_level);
	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (pylsm_error(result)) return NULL;
	Py_RETURN_TRUE;
}



static PyObject* LSM_commit(LSM *self) {
	if (self->tx_level < 0) self->tx_level = 0;
	return LSM_commit_inner(self, self->tx_level);
}


static PyObject* LSM_rollback(LSM *self) {
	if (self->tx_level < 0) self->tx_level = 0;
	return LSM_rollback_inner(self, self->tx_level);
}


static PyObject* LSM_getitem(LSM *self, PyObject *arg) {
	if (pylsm_ensure_opened(self)) return NULL;

	PyObject* key = arg;
	const char* pKey = NULL;
	Py_ssize_t nKey = 0;
	Py_ssize_t tuple_size;
	int seek_mode = LSM_SEEK_EQ;

	if (PySlice_Check(arg)) {
		PySliceObject* slice = (PySliceObject*) arg;

		LSMSliceView* view = (LSMSliceView*) LSMSliceView_new(&LSMSliceType);
		if (LSMSliceView_init(view, self, slice->start, slice->stop, slice->step)) return NULL;
		return (PyObject*) view;
	}

	if (PyTuple_Check(arg)) {
		tuple_size = PyTuple_GET_SIZE(arg);
		if (tuple_size != 2) {
			PyErr_Format(
				PyExc_ValueError,
				"tuple argument must be pair of key and seek_mode passed tuple has size %d",
				tuple_size
			);
			return NULL;
		}

		key = PyTuple_GetItem(arg, 0);
		PyObject* seek_mode_obj = PyTuple_GetItem(arg, 1);

		if (!PyLong_Check(seek_mode_obj)) {
			PyErr_Format(
				PyExc_ValueError,
				"second tuple argument must be int not %R",
				PyObject_Type(seek_mode_obj)
			);
			return NULL;
		}

		seek_mode = PyLong_AsLong(seek_mode_obj);
	}

	if (pylsm_seek_mode_check(seek_mode)) return NULL;

	if (str_or_bytes_check(self->binary, key, &pKey, &nKey)) return NULL;
	if (nKey >= INT_MAX) {
		PyErr_SetString(PyExc_OverflowError, "length of key is too large");
		return NULL;
	}

	int result;
	char *pValue = NULL;
	int nValue = 0;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);

	result = pylsm_getitem(
		self->lsm,
		pKey,
		(int) nKey,
		&pValue,
		&nValue,
		seek_mode
	);

	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (result == -1) {
		PyErr_Format(
			PyExc_KeyError,
			"Key %R was not found",
			key
		);
		if (pValue != NULL) free(pValue);
		return NULL;
	}
	if (pValue == NULL) Py_RETURN_TRUE;

	if (pylsm_error(result)) {
		if (pValue != NULL) free(pValue);
		return NULL;
	}

	PyObject* py_value = Py_BuildValue(self->binary ? "y#" : "s#", pValue, nValue);

	if (pValue != NULL) free(pValue);

	return py_value;
}


static int LSM_set_del_item(LSM* self, PyObject* key, PyObject* value) {
	if (pylsm_ensure_writable(self)) return -1;

	int rc;
	const char* pKey = NULL;
	Py_ssize_t nKey = 0;

	const char* pVal = NULL;
	Py_ssize_t nVal = 0;

	// Delete slice
	if (PySlice_Check(key)) {

		if (value != NULL) {
			PyErr_SetString(PyExc_NotImplementedError, "setting range doesn't supported yet");
			return -1;
		}

		PySliceObject* slice = (PySliceObject*) key;

		if (slice->step != Py_None) {
			PyErr_SetString(PyExc_ValueError, "Stepping not allowed in delete_range operation");
			return -1;
		}

		if (slice->start == Py_None || slice->stop == Py_None) {
			PyErr_SetString(PyExc_ValueError, "You must provide range start and range stop values");
			return -1;
		}

		char *pStop = NULL;
		char *pStart = NULL;
		Py_ssize_t nStart = 0;
		Py_ssize_t nStop = 0;

		if (str_or_bytes_check(self->binary, slice->start, (const char **) &pStart, &nStart)) return -1;
		if (str_or_bytes_check(self->binary, slice->stop, (const char **) &pStop, &nStop)) return -1;

		if (nStart >= INT_MAX) {
			PyErr_SetString(PyExc_OverflowError, "length of start is too large");
			return -1;
		}
		if (nStop >= INT_MAX) {
			PyErr_SetString(PyExc_OverflowError, "length of stop is too large");
			return -1;
		}

		Py_INCREF(slice->start);
		Py_INCREF(slice->stop);

		int rc;

		Py_BEGIN_ALLOW_THREADS
		LSM_MutexLock(self);
		rc = lsm_delete_range(
			self->lsm,
			pStart, (int) nStart,
			pStop, (int) nStop
		);
		LSM_MutexLeave(self);
		Py_END_ALLOW_THREADS

		Py_DECREF(slice->start);
		Py_DECREF(slice->stop);

		if (pylsm_error(rc)) return -1;

		return 0;
	}

	if (str_or_bytes_check(self->binary, key, &pKey, &nKey)) return -1;
	if (value != NULL) { if (str_or_bytes_check(self->binary, value, &pVal, &nVal)) return -1; }

	if (nKey >= INT_MAX) {
		PyErr_SetString(PyExc_OverflowError, "length of key is too large");
		return -1;
	}
	if (nVal >= INT_MAX) {
		PyErr_SetString(PyExc_OverflowError, "length of value is too large");
		return -1;
	}

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);
	if (pVal == NULL) {
		rc = pylsm_delitem(self->lsm, pKey, (int) nKey);
	} else {
		rc = lsm_insert(self->lsm, pKey, (int) nKey, pVal, (int) nVal);
	}
	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (rc == -1) {
		PyErr_Format(
    		PyExc_KeyError,
    		"Key %R was not found",
    		key
    	);
		return -1;
	}

	if (pylsm_error(rc)) return -1;

	return 0;
}


static int LSM_contains(LSM *self, PyObject *key) {
	if (pylsm_ensure_opened(self)) return 0;

	const char* pKey = NULL;
	Py_ssize_t nKey = 0;

	if (str_or_bytes_check(self->binary, key, (const char**) &pKey, &nKey)) return 0;

	if (nKey >= INT_MAX) {
		PyErr_SetString(PyExc_OverflowError, "length of key is too large");
		return -1;
	}

	int rc;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);
	rc = pylsm_contains(self->lsm, pKey, (int) nKey);
	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (rc == -1) return 0;
	if (rc == 0) return 1;

	pylsm_error(rc);
	return -1;
}


static PyObject* LSM_compress_get(LSM* self) {
	switch (self->lsm_compress.iId) {
		case PY_LSM_COMPRESSOR_NONE:
			Py_RETURN_NONE;
		case PY_LSM_COMPRESSOR_LZ4:
			return Py_BuildValue("s", "lz4");
		case PY_LSM_COMPRESSOR_ZSTD:
			return Py_BuildValue("s", "zstd");
	}

	PyErr_SetString(PyExc_RuntimeError, "invalid compressor");
	return NULL;
}


static PyObject* LSM_repr(LSM *self) {
	char * path = self->path;
	if (path == NULL) path = "<NULL>";
	return PyUnicode_FromFormat(
		"<%s at \"%s\" as %p>", Py_TYPE(self)->tp_name, path, self
	);
}


static Py_ssize_t LSM_length(LSM *self) {
	Py_ssize_t result = 0;
	Py_ssize_t rc = 0;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self);

	rc = pylsm_length(self->lsm, &result);
	LSM_MutexLeave(self);
	Py_END_ALLOW_THREADS

	if (pylsm_error(rc)) return -1;
	return result;
}


static LSMIterView* LSM_keys(LSM* self) {
	if (pylsm_ensure_opened(self)) return NULL;

	LSMIterView* view = (LSMIterView*) LSMIterView_new(&LSMKeysType);
	if (LSMIterView_init(view, self)) return NULL;
	return view;
}

static LSMIterView* LSM_values(LSM* self) {
	if (pylsm_ensure_opened(self)) return NULL;

	LSMIterView* view = (LSMIterView*) LSMIterView_new(&LSMValuesType);
	if (LSMIterView_init(view, self)) return NULL;
	return view;
}

static LSMIterView* LSM_items(LSM* self) {
	if (pylsm_ensure_opened(self)) return NULL;

	LSMIterView* view = (LSMIterView*) LSMIterView_new(&LSMItemsType);
	if (LSMIterView_init(view, self)) return NULL;
	return view;
}

static LSMIterView* LSM_iter(LSM* self) {
	if (pylsm_ensure_opened(self)) return NULL;

	LSMIterView* view = (LSMIterView*) LSMIterView_new(&LSMKeysType);
	if (LSMIterView_init(view, self)) return NULL;
	view = LSMIterView_iter(view);
	Py_DECREF(view);
	return view;
}

static PyObject* LSM_update(LSM* self, PyObject *args) {
	if (pylsm_ensure_writable(self)) return NULL;

	PyObject * value = NULL;

	if (!PyArg_ParseTuple(args, "O", &value)) return NULL;
	if (!PyMapping_Check(value)) {
		PyErr_Format(
			PyExc_ValueError,
			"Mapping expected not %R",
			PyObject_Type(value)
		);
		return NULL;
	}

	PyObject* items = PyMapping_Items(value);

	if (!PyList_Check(items)) {
		PyErr_Format(
			PyExc_ValueError,
			"Iterable expected not %R",
			PyObject_Type(items)
		);
		return NULL;
	}

	Py_ssize_t mapping_size = PyMapping_Length(value);

	PyObject **keys_objects = PyMem_Calloc(mapping_size, sizeof(PyObject*));
	PyObject **values_objects = PyMem_Calloc(mapping_size, sizeof(PyObject*));
	char **keys = PyMem_Calloc(mapping_size, sizeof(char*));
	char **values = PyMem_Calloc(mapping_size, sizeof(char*));
	Py_ssize_t *key_sizes = PyMem_Calloc(mapping_size, sizeof(Py_ssize_t*));
	Py_ssize_t *value_sizes = PyMem_Calloc(mapping_size, sizeof(Py_ssize_t*));

	PyObject *item;
	int count = 0;
	PyObject *iterator = PyObject_GetIter(items);

	PyObject* obj;

	unsigned short is_ok = 1;

	while ((item = PyIter_Next(iterator))) {
		if (PyTuple_Size(item) != 2) {
			Py_DECREF(item);
			PyErr_Format(
				PyExc_ValueError,
				"Mapping items must be tuple with pair not %R",
				item
			);
			is_ok = 0;
			break;
		}

		obj = PyTuple_GET_ITEM(item, 0);
		if (str_or_bytes_check(self->binary, obj, (const char**) &keys[count], &key_sizes[count])) {
			Py_DECREF(item);
			is_ok = 0;
			break;
		}

		if (key_sizes[count] >= INT_MAX) {
			PyErr_SetString(PyExc_OverflowError, "length of key is too large");
			return NULL;
		}

		keys_objects[count] = obj;
		Py_INCREF(obj);

		obj = PyTuple_GET_ITEM(item, 1);
		if (str_or_bytes_check(self->binary, obj, (const char**) &values[count], &value_sizes[count])) {
			Py_DECREF(item);
			is_ok = 0;
			break;
		}

		if (value_sizes[count] >= INT_MAX) {
			PyErr_SetString(PyExc_OverflowError, "length of value is too large");
			return NULL;
		}

		values_objects[count] = obj;
		Py_INCREF(obj);

		Py_DECREF(item);
		count++;
    }

    int rc;

	if (is_ok) {
		Py_BEGIN_ALLOW_THREADS
		LSM_MutexLock(self);
		for (int i=0; i < mapping_size; i++) {
			if ((rc = lsm_insert(self->lsm, keys[i], (int) key_sizes[i], values[i], (int) value_sizes[i]))) break;
		}
		LSM_MutexLeave(self);
		Py_END_ALLOW_THREADS

		if (pylsm_error(rc)) is_ok = 0;
	}

	for (int i = 0; i < mapping_size && keys_objects[i] != NULL; i++) Py_DECREF(keys_objects[i]);
	for (int i = 0; i < mapping_size && values_objects[i] != NULL; i++) Py_DECREF(values_objects[i]);

	PyMem_Free(key_sizes);
	PyMem_Free(value_sizes);
	PyMem_Free(keys);
	PyMem_Free(values);
	PyMem_Free(keys_objects);
	PyMem_Free(values_objects);

	Py_CLEAR(items);
	Py_CLEAR(iterator);

	if (is_ok) {
		Py_RETURN_NONE;
	} else {
		return NULL;
	}
}


static LSMTransaction* LSM_transaction(LSM* self) {
	LSM_begin(self);
	if (PyErr_Occurred()) return NULL;

	LSMTransaction* tx = (LSMTransaction*) LSMTransaction_new(&LSMTransactionType, self);
	if (PyErr_Occurred()) return NULL;

	return tx;
}


static PyMemberDef LSM_members[] = {
	{
		"path",
		T_STRING,
		offsetof(LSM, path),
		READONLY,
		"path"
	},
	{
		"compressed",
		T_BOOL,
		offsetof(LSM, compressed),
		READONLY,
		"compressed"
	},
	{
		"state",
		T_INT,
		offsetof(LSM, state),
		READONLY,
		"state"
	},
	{
		"page_size",
		T_INT,
		offsetof(LSM, page_size),
		READONLY,
		"page_size"
	},
	{
		"block_size",
		T_INT,
		offsetof(LSM, block_size),
		READONLY,
		"block_size"
	},
	{
		"safety",
		T_INT,
		offsetof(LSM, safety),
		READONLY,
		"safety"
	},
	{
		"autowork",
		T_BOOL,
		offsetof(LSM, autowork),
		READONLY,
		"autowork"
	},
	{
		"autocheckpoint",
		T_INT,
		offsetof(LSM, autocheckpoint),
		READONLY,
		"autocheckpoint"
	},
	{
		"mmap",
		T_BOOL,
		offsetof(LSM, mmap),
		READONLY,
		"mmap"
	},
	{
		"use_log",
		T_BOOL,
		offsetof(LSM, use_log),
		READONLY,
		"use_log"
	},
	{
		"automerge",
		T_INT,
		offsetof(LSM, automerge),
		READONLY,
		"automerge"
	},
	{
		"max_freelist",
		T_INT,
		offsetof(LSM, max_freelist),
		READONLY,
		"max_freelist"
	},
	{
		"multiple_processes",
		T_BOOL,
		offsetof(LSM, multiple_processes),
		READONLY,
		"multiple_processes"
	},
	{
		"readonly",
		T_BOOL,
		offsetof(LSM, readonly),
		READONLY,
		"readonly"
	},
	{
		"binary",
		T_BOOL,
		offsetof(LSM, binary),
		READONLY,
		"binary"
	},
	{
		"compress_level",
		T_INT,
		offsetof(LSM, compress_level),
		READONLY,
		"compress_level"
	},
	{
		"tx_level",
		T_INT,
		offsetof(LSM, tx_level),
		READONLY,
		"Transaction nesting level"
	},
	{NULL}  /* Sentinel */
};


static PyMethodDef LSM_methods[] = {
	{
		"__enter__",
		(PyCFunction) LSM_ctx_enter, METH_NOARGS,
		"Enter context"
	},
	{
		"__exit__",
		(PyCFunction) LSM_ctx_exit, METH_VARARGS | METH_KEYWORDS,
		"Exit context"
	},
	{
		"open",
		(PyCFunction) LSM_open, METH_NOARGS,
		"Open database"
	},
	{
		"close",
		(PyCFunction) LSM_close, METH_NOARGS,
		"Close database"
	},
	{
		"info",
		(PyCFunction) LSM_info, METH_NOARGS,
		"Database info"
	},
	{
		"work",
		(PyCFunction) LSM_work, METH_VARARGS | METH_KEYWORDS,
		"Explicit Database work"
	},
	{
		"flush",
		(PyCFunction) LSM_flush, METH_NOARGS,
		"Explicit Database flush"
	},
	{
		"checkpoint",
		(PyCFunction) LSM_checkpoint, METH_NOARGS,
		"Explicit Database checkpointing"
	},
	{
		"cursor",
		(PyCFunction) LSM_cursor, METH_VARARGS | METH_KEYWORDS,
		"Create a cursor"
	},
	{
		"insert",
		(PyCFunction) LSM_insert, METH_VARARGS | METH_KEYWORDS,
		"Insert key and value"
	},
	{
		"delete",
		(PyCFunction) LSM_delete, METH_VARARGS | METH_KEYWORDS,
		"Delete value by key"
	},
	{
		"delete_range",
		(PyCFunction) LSM_delete_range, METH_VARARGS | METH_KEYWORDS,
		"Delete values by range"
	},
	{
		"begin",
		(PyCFunction) LSM_begin, METH_NOARGS,
		"Start transaction"
	},
	{
		"commit",
		(PyCFunction) LSM_commit, METH_NOARGS,
		"Commit transaction"
	},
	{
		"rollback",
		(PyCFunction) LSM_rollback, METH_NOARGS,
		"Rollback transaction"
	},
	{
		"transaction",
		(PyCFunction) LSM_transaction, METH_NOARGS,
		"Return transaction instance"
	},
	{
		"tx",
		(PyCFunction) LSM_transaction, METH_NOARGS,
		"Alias of transaction method"
	},
	{
		"keys",
		(PyCFunction) LSM_keys, METH_NOARGS,
		"Returns lsm_keys instance"
	},
	{
		"values",
		(PyCFunction) LSM_values, METH_NOARGS,
		"Returns lsm_keys instance"
	},
	{
		"items",
		(PyCFunction) LSM_items, METH_NOARGS,
		"Returns lsm_keys instance"
	},
	{
		"update",
		(PyCFunction) LSM_update, METH_VARARGS,
		"dict-like update method"

	},
	{NULL}  /* Sentinel */
};


static PyGetSetDef LSMTypeGetSet[] = {
	{
		.name = "compress",
		.get = (PyObject *(*)(PyObject *, void *)) LSM_compress_get,
		.doc = "Compression algorithm"
	},
	{NULL} /* Sentinel */
};

static PyMappingMethods LSMTypeMapping = {
	.mp_subscript = (binaryfunc) LSM_getitem,
	.mp_ass_subscript = (objobjargproc) LSM_set_del_item,
	.mp_length = (lenfunc) LSM_length
};


static PySequenceMethods LSMTypeSequence = {
	.sq_contains = (objobjproc) LSM_contains
};


static PyTypeObject LSMType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "LSM",
	.tp_doc = "",
	.tp_basicsize = sizeof(LSM),
	.tp_itemsize = 0,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_new = LSM_new,
	.tp_init = (initproc) LSM_init,
	.tp_dealloc = (destructor) LSM_dealloc,
	.tp_members = LSM_members,
	.tp_methods = LSM_methods,
	.tp_repr = (reprfunc) LSM_repr,
	.tp_as_mapping = &LSMTypeMapping,
	.tp_as_sequence = &LSMTypeSequence,
	.tp_getset = (struct PyGetSetDef *) &LSMTypeGetSet,
	.tp_iter = (getiterfunc) LSM_iter,
	.tp_weaklistoffset = offsetof(LSM, weakrefs)
};


static PyObject* LSMCursor_new(PyTypeObject *type, LSM *db, int seek_mode) {
	if (pylsm_ensure_opened(db)) return NULL;

	LSMCursor *self;

	self = (LSMCursor *) type->tp_alloc(type, 0);
	self->state = PY_LSM_INITIALIZED;
	self->db = db;

	self->seek_mode = seek_mode;

	int rc;

	LSM_MutexLock(db);
	rc = lsm_csr_open(self->db->lsm, &self->cursor);
	LSM_MutexLeave(db);

	if(pylsm_error(rc)) return NULL;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self->db);
	rc = lsm_csr_first(self->cursor);
	LSM_MutexLeave(self->db);
	Py_END_ALLOW_THREADS

	if (pylsm_error(rc)) return NULL;

	self->state = PY_LSM_OPENED;

	Py_INCREF(self->db);

	return (PyObject *) self;
}


static void LSMCursor_dealloc(LSMCursor *self) {
	if (self->state != PY_LSM_CLOSED && self->cursor != NULL) {
		lsm_csr_close(self->cursor);
		self->cursor = NULL;
		self->state = PY_LSM_CLOSED;
	}

	if (self->db != NULL) {
		Py_DECREF(self->db);
		self->db = NULL;
	}

	if (self->weakrefs != NULL) PyObject_ClearWeakRefs((PyObject *) self);
}


static PyObject* LSMCursor_first(LSMCursor *self) {
	if (self->state == PY_LSM_ITERATING) {
		PyErr_SetString(PyExc_RuntimeError, "can not change cursor during iteration");
		return NULL;
	}

	if (pylsm_ensure_csr_opened(self)) return NULL;
	int result;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self->db);
	result = lsm_csr_first(self->cursor);
	LSM_MutexLeave(self->db);
	Py_END_ALLOW_THREADS

	if (pylsm_error(result)) return NULL;
	self->state = PY_LSM_OPENED;

	if (!lsm_csr_valid(self->cursor)) Py_RETURN_FALSE;

	Py_RETURN_TRUE;
}


static PyObject* LSMCursor_last(LSMCursor *self) {
	if (self->state == PY_LSM_ITERATING) {
		PyErr_SetString(PyExc_RuntimeError, "can not change cursor during iteration");
		return NULL;
	}
	if (pylsm_ensure_csr_opened(self)) return NULL;
	int result;

	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self->db);
	result = lsm_csr_last(self->cursor);
	LSM_MutexLeave(self->db);
	Py_END_ALLOW_THREADS

	if (pylsm_error(result)) return NULL;
	self->state = PY_LSM_OPENED;

	if (!lsm_csr_valid(self->cursor)) Py_RETURN_FALSE;

	Py_RETURN_TRUE;
}


static PyObject* LSMCursor_close(LSMCursor *self) {
	if (pylsm_ensure_csr_opened(self)) return NULL;
	int result;
	result = lsm_csr_close(self->cursor);

	if (pylsm_error(result)) return NULL;

	if (self->db != NULL) Py_DECREF(self->db);
	self->db = NULL;

	self->cursor = NULL;
	self->state = PY_LSM_CLOSED;
	Py_RETURN_NONE;
}

static PyObject* LSMCursor_seek(LSMCursor *self, PyObject* args, PyObject* kwds) {
	if (self->state == PY_LSM_ITERATING) {
		PyErr_SetString(PyExc_RuntimeError, "can not change cursor during iteration");
		return NULL;
	}

	if (pylsm_ensure_csr_opened(self)) return NULL;
	static char *kwlist[] = {"key", "seek_mode", NULL};

	self->seek_mode = LSM_SEEK_EQ;

	PyObject* key = NULL;
	const char* pKey = NULL;
	Py_ssize_t nKey = 0;

	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|i", kwlist, &key, &self->seek_mode)) return NULL;
	if (pylsm_seek_mode_check(self->seek_mode)) return NULL;

	int rc;

	if (str_or_bytes_check(self->db->binary, key, &pKey, &nKey)) return NULL;

	if (nKey >= INT_MAX) {
		PyErr_SetString(PyExc_OverflowError, "length of key is too large");
		return NULL;
	}
	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self->db);
	rc = lsm_csr_seek(self->cursor, pKey, (int) nKey, self->seek_mode);
	LSM_MutexLeave(self->db);
	Py_END_ALLOW_THREADS

	if (pylsm_error(rc)) return NULL;
	if (lsm_csr_valid(self->cursor)) { Py_RETURN_TRUE; } else { Py_RETURN_FALSE; }
}


static PyObject* LSMCursor_compare(LSMCursor *self, PyObject* args, PyObject* kwds) {
	if (pylsm_ensure_csr_opened(self)) return NULL;

	if (!lsm_csr_valid(self->cursor)) {
		PyErr_SetString(PyExc_RuntimeError, "Invalid cursor");
		return NULL;
	};

	static char *kwlist[] = {"key", NULL};

	PyObject * key = NULL;
	const char* pKey = NULL;
	Py_ssize_t nKey = 0;

	if (!PyArg_ParseTupleAndKeywords(args, kwds, "O", kwlist, &key)) return NULL;
	if (str_or_bytes_check(self->db->binary, key, &pKey, &nKey)) return NULL;

	int cmp_result = 0;
	int result;

	if (nKey >= INT_MAX) {
		PyErr_SetString(PyExc_OverflowError, "length of key is too large");
		return NULL;
	}

	LSM_MutexLock(self->db);
	result = lsm_csr_cmp(self->cursor, pKey, (int) nKey, &cmp_result);
	LSM_MutexLeave(self->db);

	if (self->seek_mode == LSM_SEEK_GE) cmp_result = -cmp_result;

	if (pylsm_error(result)) return NULL;
	return Py_BuildValue("i", cmp_result);
}

static PyObject* LSMCursor_retrieve(LSMCursor *self) {
	if (self->state == PY_LSM_ITERATING) {
		PyErr_SetString(PyExc_RuntimeError, "can not change cursor during iteration");
		return NULL;
	}
	if (pylsm_ensure_csr_opened(self)) return NULL;
	if(!lsm_csr_valid(self->cursor)) { Py_RETURN_NONE; }

	LSM_MutexLock(self->db);
	PyObject* result = pylsm_cursor_items_fetch(self->cursor, self->db->binary);
	LSM_MutexLeave(self->db);
	return result;
}


static PyObject* LSMCursor_key(LSMCursor *self) {
	if (self->state == PY_LSM_ITERATING) {
		PyErr_SetString(PyExc_RuntimeError, "can not change cursor during iteration");
		return NULL;
	}
	if (pylsm_ensure_csr_opened(self)) return NULL;
	if(!lsm_csr_valid(self->cursor)) { Py_RETURN_NONE; }

	LSM_MutexLock(self->db);
	PyObject* result = pylsm_cursor_key_fetch(self->cursor, self->db->binary);
	LSM_MutexLeave(self->db);

	return result;
}


static PyObject* LSMCursor_value(LSMCursor *self) {
	if (self->state == PY_LSM_ITERATING) {
		PyErr_SetString(PyExc_RuntimeError, "can not change cursor during iteration");
		return NULL;
	}
	if (pylsm_ensure_csr_opened(self)) return NULL;
	if(!lsm_csr_valid(self->cursor)) { Py_RETURN_NONE; }

	LSM_MutexLock(self->db);
	PyObject* result = pylsm_cursor_value_fetch(self->cursor, self->db->binary);
	LSM_MutexLeave(self->db);

	return result;
}


static PyObject* LSMCursor_next(LSMCursor *self) {
	if (self->state == PY_LSM_ITERATING) {
		PyErr_SetString(PyExc_RuntimeError, "can not change cursor during iteration");
		return NULL;
	}
	if (pylsm_ensure_csr_opened(self)) return NULL;
	if (self->seek_mode == LSM_SEEK_EQ) Py_RETURN_FALSE;
	if (!lsm_csr_valid(self->cursor)) Py_RETURN_FALSE;

	int err;
	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self->db);
	err = lsm_csr_next(self->cursor);
	LSM_MutexLeave(self->db);
	Py_END_ALLOW_THREADS

	if (pylsm_error(err)) return NULL;

	if (!lsm_csr_valid(self->cursor)) Py_RETURN_FALSE;
	Py_RETURN_TRUE;
}


static PyObject* LSMCursor_previous(LSMCursor *self) {
	if (self->state == PY_LSM_ITERATING) {
		PyErr_SetString(PyExc_RuntimeError, "can not change cursor during iteration");
		return NULL;
	}
	if (pylsm_ensure_csr_opened(self)) return NULL;
	if (self->seek_mode == LSM_SEEK_EQ) {
		PyErr_SetString(PyExc_RuntimeError, "can not seek in SEEK_EQ mode");
		return NULL;
	};

	if (!lsm_csr_valid(self->cursor)) Py_RETURN_FALSE;

	int err;
	Py_BEGIN_ALLOW_THREADS
	LSM_MutexLock(self->db);
	err = lsm_csr_prev(self->cursor);
	LSM_MutexLeave(self->db);
	Py_END_ALLOW_THREADS

	if (pylsm_error(err)) return NULL;
	if (!lsm_csr_valid(self->cursor)) Py_RETURN_FALSE;
	Py_RETURN_TRUE;
}


static PyObject* LSMCursor_ctx_enter(LSMCursor *self) {
	if (self->state == PY_LSM_ITERATING) {
		PyErr_SetString(PyExc_RuntimeError, "can not change cursor during iteration");
		return NULL;
	}
	if (pylsm_ensure_csr_opened(self)) return NULL;
	return (PyObject*) self;
}


static PyObject* LSMCursor_ctx_exit(LSMCursor *self) {
	if (self->state == PY_LSM_CLOSED) { Py_RETURN_NONE; };

	LSMCursor_close(self);
	if (PyErr_Occurred()) return NULL;

	Py_RETURN_NONE;
}


static PyObject* LSMCursor_repr(LSMCursor *self) {
	return PyUnicode_FromFormat(
		"<%s as %p>",
		Py_TYPE(self)->tp_name, self
	);
}


static PyMemberDef LSMCursor_members[] = {
	{
		"state",
		T_INT,
		offsetof(LSMCursor, state),
		READONLY,
		"state"
	},
	{
		"seek_mode",
		T_INT,
		offsetof(LSMCursor, seek_mode),
		READONLY,
		"seek_mode"
	},
	{NULL}  /* Sentinel */
};


static PyMethodDef LSMCursor_methods[] = {
	{
		"__enter__",
		(PyCFunction) LSMCursor_ctx_enter, METH_NOARGS,
		"Enter context"
	},
	{
		"__exit__",
		(PyCFunction) LSMCursor_ctx_exit, METH_VARARGS | METH_KEYWORDS,
		"Exit context"
	},
	{
		"close",
		(PyCFunction) LSMCursor_close, METH_NOARGS,
		"Close database"
	},
	{
		"first",
		(PyCFunction) LSMCursor_first, METH_NOARGS,
		"Move cursor to first item"
	},
	{
		"last",
		(PyCFunction) LSMCursor_last, METH_NOARGS,
		"Move cursor to last item"
	},
	{
		"seek",
		(PyCFunction) LSMCursor_seek, METH_VARARGS | METH_KEYWORDS,
		"Seek to key"
	},
	{
		"retrieve",
		(PyCFunction) LSMCursor_retrieve, METH_NOARGS,
		"Retrieve key and value"
	},
	{
		"key",
		(PyCFunction) LSMCursor_key, METH_NOARGS,
		"Retrieve key"
	},
	{
		"value",
		(PyCFunction) LSMCursor_value, METH_NOARGS,
		"Retrieve value"
	},
	{
		"next",
		(PyCFunction) LSMCursor_next, METH_NOARGS,
		"Seek next"
	},
	{
		"previous",
		(PyCFunction) LSMCursor_previous, METH_NOARGS,
		"Seek previous"
	},
	{
		"compare",
		(PyCFunction) LSMCursor_compare, METH_VARARGS | METH_KEYWORDS,
		"Compare current position against key"
	},

	{NULL}  /* Sentinel */
};

static PyTypeObject LSMCursorType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "Cursor",
	.tp_doc = "",
	.tp_basicsize = sizeof(LSMCursor),
	.tp_itemsize = 0,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_dealloc = (destructor) LSMCursor_dealloc,
	.tp_members = LSMCursor_members,
	.tp_methods = LSMCursor_methods,
	.tp_repr = (reprfunc) LSMCursor_repr,
	.tp_weaklistoffset = offsetof(LSMCursor, weakrefs)
};


static PyObject* LSMTransaction_new(PyTypeObject *type, LSM* db) {
	LSMTransaction *self;

	self = (LSMTransaction *) type->tp_alloc(type, 0);
	self->state = PY_LSM_INITIALIZED;
	self->db = db;
	self->tx_level = self->db->tx_level;

	Py_INCREF(self->db);

	return (PyObject *) self;
}


static void LSMTransaction_dealloc(LSMTransaction *self) {
	if (self->weakrefs != NULL) PyObject_ClearWeakRefs((PyObject *) self);
	if (self->db == NULL) return;

	Py_DECREF(self->db);
	if (self->state != PY_LSM_CLOSED && self->db->state != PY_LSM_CLOSED) {
		LSM_rollback_inner(self->db, self->tx_level);
	}
}


static PyObject* LSMTransaction_ctx_enter(LSMTransaction *self) {
	if (pylsm_ensure_writable(self->db)) return NULL;
	return (PyObject*) self;
}


static PyObject* LSMTransaction_commit(LSMTransaction *self);
static PyObject* LSMTransaction_rollback(LSMTransaction *self);


static PyObject* LSMTransaction_ctx_exit(
	LSMTransaction *self, PyObject *const *args
) {
	if (self->state == PY_LSM_CLOSED) Py_RETURN_NONE;

	PyObject *exc_type, *exc_value, *exc_tb;
    if (!PyArg_ParseTuple(args, "OOO", &exc_type, &exc_value, &exc_tb)) return NULL;

	self->state = PY_LSM_CLOSED;

	if (exc_type == Py_None) {
		LSM_commit_inner(self->db, self->tx_level - 1);
	} else {
		LSM_rollback_inner(self->db, self->tx_level);
	}

	if (PyErr_Occurred()) return NULL;

	Py_RETURN_NONE;
}


static PyObject* LSMTransaction_commit(LSMTransaction *self) {
	PyObject * result = LSM_commit_inner(self->db, self->tx_level -1);
	if (PyErr_Occurred()) return NULL;
	if (pylsm_error(lsm_begin(self->db->lsm, self->tx_level))) return NULL;
	return result;
}


static PyObject* LSMTransaction_rollback(LSMTransaction *self) {
	return LSM_rollback_inner(self->db, self->tx_level);
}



static PyMemberDef LSMTransaction_members[] = {
	{
		"level",
		T_INT,
		offsetof(LSMTransaction, tx_level),
		READONLY,
		"Transaction level"
	},
	{NULL}  /* Sentinel */
};


static PyMethodDef LSMTransaction_methods[] = {
	{
		"__enter__",
		(PyCFunction) LSMTransaction_ctx_enter, METH_NOARGS,
		"Enter context"
	},
	{
		"__exit__",
		(PyCFunction) LSMTransaction_ctx_exit, METH_VARARGS,
		"Exit context"
	},
	{
		"commit",
		(PyCFunction) LSMTransaction_commit, METH_NOARGS,
		"Commit transaction"
	},
	{
		"rollback",
		(PyCFunction) LSMTransaction_rollback, METH_NOARGS,
		"Rollback transaction"
	},

	{NULL}  /* Sentinel */
};

static PyTypeObject LSMTransactionType = {
	PyVarObject_HEAD_INIT(NULL, 0)
	.tp_name = "Transaction",
	.tp_doc = "",
	.tp_basicsize = sizeof(LSMTransaction),
	.tp_itemsize = 0,
	.tp_flags = Py_TPFLAGS_DEFAULT,
	.tp_dealloc = (destructor) LSMTransaction_dealloc,
	.tp_methods = LSMTransaction_methods,
	.tp_members = LSMTransaction_members,
	.tp_weaklistoffset = offsetof(LSMTransaction, weakrefs)
};


static PyModuleDef lsm_module = {
	PyModuleDef_HEAD_INIT,
	.m_name = "lsm",
	.m_doc = "LSM DB python binding",
	.m_size = -1,
};


PyMODINIT_FUNC PyInit_lsm(void) {
	PyObject *m;

	m = PyModule_Create(&lsm_module);

	if (m == NULL) return NULL;

	if (PyType_Ready(&LSMType) < 0) return NULL;
	Py_INCREF(&LSMType);

	if (PyModule_AddObject(m, "LSM", (PyObject *) &LSMType) < 0) {
		Py_XDECREF(&LSMType);
		Py_XDECREF(m);
		return NULL;
	}

	if (PyType_Ready(&LSMCursorType) < 0) return NULL;
	Py_INCREF(&LSMCursorType);

	if (PyModule_AddObject(m, "Cursor", (PyObject *) &LSMCursorType) < 0) {
		Py_XDECREF(&LSMCursorType);
		Py_XDECREF(m);
		return NULL;
	}

	if (PyType_Ready(&LSMTransactionType) < 0) return NULL;
	Py_INCREF(&LSMTransactionType);

	if (PyModule_AddObject(m, "Transaction", (PyObject *) &LSMTransactionType) < 0) {
		Py_XDECREF(&LSMTransactionType);
		Py_XDECREF(m);
		return NULL;
	}

	if (PyType_Ready(&LSMItemsType) < 0) return NULL;
	Py_INCREF(&LSMItemsType);

	if (PyType_Ready(&LSMValuesType) < 0) return NULL;
	Py_INCREF(&LSMValuesType);

	if (PyType_Ready(&LSMKeysType) < 0) return NULL;
	Py_INCREF(&LSMKeysType);

	if (PyType_Ready(&LSMSliceType) < 0) return NULL;
	Py_INCREF(&LSMSliceType);

	PyModule_AddIntConstant(m, "SAFETY_OFF", LSM_SAFETY_OFF);
	PyModule_AddIntConstant(m, "SAFETY_NORMAL", LSM_SAFETY_NORMAL);
	PyModule_AddIntConstant(m, "SAFETY_FULL", LSM_SAFETY_FULL);

	PyModule_AddIntConstant(m, "STATE_INITIALIZED", PY_LSM_INITIALIZED);
	PyModule_AddIntConstant(m, "STATE_OPENED", PY_LSM_OPENED);
	PyModule_AddIntConstant(m, "STATE_CLOSED", PY_LSM_CLOSED);

	PyModule_AddIntConstant(m, "SEEK_EQ", LSM_SEEK_EQ);
	PyModule_AddIntConstant(m, "SEEK_LE", LSM_SEEK_LE);
	PyModule_AddIntConstant(m, "SEEK_GE", LSM_SEEK_GE);
	PyModule_AddIntConstant(m, "SEEK_LEFAST", LSM_SEEK_LEFAST);

	return m;
}
