import os
import platform

from setuptools import Extension, setup


module_name = "lsm"


define_macros = []
compiller_args = []
libraries = []


if platform.system() in ("Darwin", "Linux"):
    define_macros.append(('LSM_MUTEX_PTHREADS', None))
    compiller_args += (
        "-g3",
        "-std=c99",
        "-O0",
        "-fPIC",
        "-Wall",
        "-ftrapv",
        "-fwrapv",
    )
    libraries.append("pthread")


if platform.system() in ("Windows",):
    define_macros.append(('LSM_MUTEX_WIN32', None))


sources = {
    "sqlite/ext/lsm1": [
        "lsm_main.c",
        "lsm_win32.c",
        "lsm_file.c",
        "lsm_tree.c",
        "lsm_log.c",
        "lsm_ckpt.c",
        "lsm_mutex.c",
        "lsm_mem.c",
        "lsm_str.c",
        "lsm_unix.c",
        "lsm_varint.c",
        "lsm_shared.c",
        "lsm_sorted.c",
    ],
    "lz4/lib": ["lz4.c",],
    "zstd/lib": [
        "compress/zstd_compress.c",
        "compress/zstd_compress_literals.c",
        "compress/zstd_compress_sequences.c",
        "compress/zstd_compress_superblock.c",
        "compress/zstdmt_compress.c",
        "compress/zstd_fast.c",
        "compress/zstd_double_fast.c",
        "compress/zstd_lazy.c",
        "compress/zstd_opt.c",
        "compress/zstd_ldm.c",
        "compress/fse_compress.c",
        "compress/huf_compress.c",
        "compress/hist.c",
        "common/fse_decompress.c",
        "decompress/zstd_decompress.c",
        "decompress/zstd_decompress_block.c",
        "decompress/zstd_ddict.c",
        "decompress/huf_decompress.c",
        "common/entropy_common.c",
        "common/zstd_common.c",
        "common/xxhash.c",
        "common/error_private.c",
        "common/pool.c",
        "common/threading.c",
    ],
    "": ["lsm.c"]
}


def library_sources():
    result = []
    for parent_dir, files in sources.items():
        result += [os.path.join("src", parent_dir, f) for f in files]
    return result


setup(
    name=module_name,
    version="0.4.6",
    ext_modules=[
        Extension(
            "lsm",
            library_sources(),
            undef_macros=["NDEBUG"],
            define_macros=define_macros,
            libraries=libraries,
            extra_compile_args=compiller_args,
        ),
    ],
    include_package_data=True,
    description="Python bindings for SQLite's LSM key/value engine",
    long_description=open("README.md").read(),
    long_description_content_type='text/markdown',
    license="Apache Software License",
    author="Dmitry Orlov",
    author_email="me@mosquito.su",
    url="https://github.com/mosquito/python-lsm/",
    project_urls={
        "Documentation": "https://github.com/mosquito/python-lsm/",
        "Source": "https://github.com/mosquito/python-lsm/",
        "Tracker": "https://github.com/mosquito/python-lsm/issues",
        "Say Thanks!": "https://saythanks.io/to/mosquito",
    },
    packages=['', 'lsm_tool'],
    package_data={'': ["lsm.pyi"]},
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: MacOS",
        "Operating System :: Microsoft",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development",
    ],
    entry_points={
        "console_scripts": [
            "lsm-tool = lsm_tool:main"
        ]
    },
    python_requires=">=3.7.*, <4",
    extras_require={
        "develop": [
            "pytest",
            "pytest-subtests",
        ],
    },
)
