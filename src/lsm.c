#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include <unistd.h>

#include "lsm1/lsm.h"

#define IS_64_BIT (sizeof(void*)==8)


typedef struct {
	PyObject_HEAD
	lsm_db      *lsm;
	uint8_t     state;
    uint32_t    autoflush;
    uint32_t    page_size;
    uint32_t    block_size;
    uint8_t     safety;
    uint32_t    autowork;
    uint32_t    mmap;
    uint8_t         use_log;
    uint32_t    automerge;
    uint32_t    max_freelist;
    uint8_t     multiple_processes;
    uint32_t    autocheckpoint;
    uint8_t     readonly;
//    TODO: Support compression
//    PyObject    *compressor;
//    PyObject    *decompressor;
} LSM;

#define LSM_MAX_AUTOFLUSH 1048576

static enum PY_LSM_STATE {
    PY_LSM_INITIALIZED = 0,
    PY_LSM_OPENED = 1,
    PY_LSM_CLOSED = 2
};


static int lsm_error(int rc) {
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
            PyErr_SetNone(PyExc_MemoryError);
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


static uint8_t is_power_of_two(int n) {
   if (n==0) return 0;
   return (ceil(log2(n)) == floor(log2(n)));
}


static PyObject *
LSM_new(PyTypeObject *type, PyObject *args, PyObject *kwds) {
	LSM *self;

	self = (LSM *) type->tp_alloc(type, 0);
	return (PyObject *) self;
}


static void LSM_dealloc(LSM *self) {
    if (self->state != PY_LSM_CLOSED && self->lsm != NULL) {
        lsm_close(self->lsm);
    }
}



static int LSM_init(LSM *self, PyObject *args, PyObject *kwds) {
	static char *kwlist[] = {
	    "autoflush",
	    "page_size",
        "safety",
        "block_size",
        "autowork",
        "mmap",
        "use_log",
        "automerge",
        "max_freelist",
        "multiple_processes",
        "autocheckpoint",
        "readonly",
//        TODO: Support compression
//        "compressor",
//        "decompressor",
        NULL
    };

    self->autocheckpoint = 2048;
    self->autoflush = 1024;
    self->automerge = 4;
    self->autowork = 1;
    self->block_size = 1024;
    self->max_freelist = 24;
    self->mmap = (IS_64_BIT ? 1 : 32768);
    self->multiple_processes = 1;
    self->page_size = 4 * 1024;
    self->readonly = 0;
    self->safety = LSM_SAFETY_NORMAL;
    self->use_log = 1;

    if (!PyArg_ParseTupleAndKeywords(
        args, kwds, "|IIIBIIpIIpIp", kwlist,
        self->autoflush,
        self->page_size,
        self->safety,
        self->block_size,
        self->autowork,
        self->mmap,
        self->use_log,
        self->automerge,
        self->max_freelist,
        self->multiple_processes,
        self->autocheckpoint,
        self->readonly
    )) return -1;

    self->state = PY_LSM_INITIALIZED;

    if (self->autoflush > LSM_MAX_AUTOFLUSH) {
        PyErr_Format(
            PyExc_ValueError,
            "The maximum allowable value for autoflush parameter "
            "is 1048576 (1GB). Not %d",
            self->autoflush
        );
        return -1;
    }

    if (self->autocheckpoint == 0) {
        PyErr_SetString(
            PyExc_ValueError,
            "autocheckpoint is not able to be zero"
        );
        return -1;
    }

    if (!(is_power_of_two(self->block_size) && self->block_size > 64 && self->block_size < 65536)) {
        PyErr_Format(
            PyExc_ValueError,
            "block_size parameter must be power of two between "
            "64 and 65536. Not %d",
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

    if (lsm_error(lsm_new(NULL, &self->lsm))) return -1;

    if (self->lsm == NULL) {
        PyErr_SetString(PyExc_MemoryError, "Can not allocate memory");
        return -1;
    }

    // Only before lsm_open
    if (lsm_error(lsm_config(self->lsm, LSM_CONFIG_BLOCK_SIZE, &self->block_size))) return -1;
    if (lsm_error(lsm_config(self->lsm, LSM_CONFIG_MULTIPLE_PROCESSES, &self->multiple_processes))) return -1;
    if (lsm_error(lsm_config(self->lsm, LSM_CONFIG_PAGE_SIZE, &self->page_size))) return -1;
    if (lsm_error(lsm_config(self->lsm, LSM_CONFIG_READONLY, &self->readonly))) return -1;

    // Not only before lsm_open
    if (lsm_error(lsm_config(self->lsm, LSM_CONFIG_AUTOCHECKPOINT, &self->autocheckpoint))) return -1;
    if (lsm_error(lsm_config(self->lsm, LSM_CONFIG_AUTOFLUSH, &self->autoflush))) return -1;
    if (lsm_error(lsm_config(self->lsm, LSM_CONFIG_AUTOMERGE, &self->automerge))) return -1;
    if (lsm_error(lsm_config(self->lsm, LSM_CONFIG_AUTOWORK, &self->autowork))) return -1;
    if (lsm_error(lsm_config(self->lsm, LSM_CONFIG_MAX_FREELIST, &self->max_freelist))) return -1;
    if (lsm_error(lsm_config(self->lsm, LSM_CONFIG_MMAP, &self->mmap))) return -1;
    if (lsm_error(lsm_config(self->lsm, LSM_CONFIG_SAFETY, &self->safety))) return -1;
    if (lsm_error(lsm_config(self->lsm, LSM_CONFIG_USE_LOG, &self->autoflush))) return -1;

	return 0;
}


static PyObject* LSM_open(LSM *self, PyObject *args, PyObject *kwds) {
    if (self->state == PY_LSM_OPENED) {
        PyErr_SetString(PyExc_RuntimeError, "Database already opened");
        return NULL;
    }

    if (self->state == PY_LSM_CLOSED) {
        PyErr_SetString(PyExc_RuntimeError, "Database closed");
        return NULL;
    }

	static char *kwlist[] = {"path", NULL};

    char* path = NULL;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s", kwlist, &path)) return NULL;

    int result;

    Py_BEGIN_ALLOW_THREADS
    result = lsm_open(self->lsm, path);
    Py_END_ALLOW_THREADS

    if (lsm_error(result)) return NULL;
    self->state = PY_LSM_OPENED;

    Py_RETURN_TRUE;
}


static PyObject* LSM_close(LSM *self) {
    if (self->state == PY_LSM_CLOSED) {
        PyErr_SetString(PyExc_RuntimeError, "Database already closed");
        return NULL;
    }

    int result;
    Py_BEGIN_ALLOW_THREADS
    result = lsm_close(self->lsm);
    Py_END_ALLOW_THREADS

    if (lsm_error(result)) return NULL;

    self->lsm = NULL;
	self->state = PY_LSM_OPENED;

    Py_RETURN_TRUE;
}


static PyObject* LSM_info(LSM *self) {
    if (self->state != PY_LSM_OPENED) {
        PyErr_SetString(PyExc_RuntimeError, "Invalid state");
        return NULL;
    }

    int32_t nwrite; int nwrite_result;
    int32_t nread; int nread_result;
    int checkpoint_size, checkpoint_size_result;
    int tree_size_old, tree_size_current, tree_size_result;

    char **db_structure; int db_structure_result;

    Py_BEGIN_ALLOW_THREADS
    nwrite_result = lsm_info(
        self->lsm, LSM_INFO_NWRITE, &nwrite
    );
    nread_result = lsm_info(
        self->lsm, LSM_INFO_NREAD, &nread
    );
    checkpoint_size_result = lsm_info(
        self->lsm, LSM_INFO_CHECKPOINT_SIZE, &checkpoint_size
    );
    tree_size_result = lsm_info(
        self->lsm, LSM_INFO_TREE_SIZE, &tree_size_old, &tree_size_current
    );

    Py_END_ALLOW_THREADS

    if (lsm_error(nwrite_result)) return NULL;
    if (lsm_error(nread_result)) return NULL;
    if (lsm_error(checkpoint_size_result)) return NULL;
    if (lsm_error(tree_size_result)) return NULL;

    PyObject *result = PyDict_New();
    PyObject *val;

    if (PyDict_SetItemString(result, "nwrite", PyLong_FromLong(nwrite))) return NULL;
    if (PyDict_SetItemString(result, "nread", PyLong_FromLongLong(nread))) return NULL;
    if (PyDict_SetItemString(result, "checkpoint_size_result", PyLong_FromLong(checkpoint_size))) return NULL;

    PyObject *tree_size = PyDict_New();
    if (PyDict_SetItemString(tree_size, "old", PyLong_FromLong(tree_size_old))) return NULL;
    if (PyDict_SetItemString(tree_size, "current", PyLong_FromLong(tree_size_current))) return NULL;
    if (PyDict_SetItemString(result, "tree_size", tree_size)) return NULL;

    return result;
}


static PyObject* LSM_repr(LSM *self) {
	return PyUnicode_FromFormat(
        "<%s as %p>",
        Py_TYPE(self)->tp_name, self
    );
}


static PyMemberDef LSM_members[] = {
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
        T_INT,
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
        T_INT,
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

	{NULL}  /* Sentinel */
};


static PyMethodDef LSM_methods[] = {
    {
        "open",
        (PyCFunction) LSM_open, METH_VARARGS | METH_KEYWORDS,
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
	{NULL}  /* Sentinel */
};

static PyTypeObject
LSMType = {
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
	.tp_repr = (reprfunc) LSM_repr
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

    PyModule_AddIntConstant(m, "SAFETY_OFF", LSM_SAFETY_OFF);
    PyModule_AddIntConstant(m, "SAFETY_NORMAL", LSM_SAFETY_NORMAL);
    PyModule_AddIntConstant(m, "SAFETY_FULL", LSM_SAFETY_FULL);

    PyModule_AddIntConstant(m, "STATE_INITIALIZED", PY_LSM_INITIALIZED);
    PyModule_AddIntConstant(m, "STATE_OPENED", PY_LSM_OPENED);
    PyModule_AddIntConstant(m, "STATE_CLOSED", PY_LSM_CLOSED);

	return m;
}
