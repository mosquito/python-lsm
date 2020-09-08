import os
from importlib.machinery import SourceFileLoader

from setuptools import Extension, setup


module_name = "lsm"

setup(
    name=module_name,
    version="0.1.0",
    ext_modules=[
        Extension(
            "lsm",
            [
                "src/lsm1/lsm_main.c",
                "src/lsm1/lsm_win32.c",
                "src/lsm1/lsm_file.c",
                "src/lsm1/lsm_tree.c",
                "src/lsm1/lsm_log.c",
                "src/lsm1/lsm_ckpt.c",
                "src/lsm1/lsm_mutex.c",
                "src/lsm1/lsm_mem.c",
                "src/lsm1/lsm_vtab.c",
                "src/lsm1/lsm_str.c",
                "src/lsm1/lsm_unix.c",
                "src/lsm1/lsm_varint.c",
                "src/lsm1/lsm_shared.c",
                "src/lsm1/lsm_sorted.c",
                "src/lsm.c",
            ],
            libraries=[],
            extra_compile_args=[],
        ),
    ],
    include_package_data=True,
    description="Python bindings for SQLite's LSM key/value store",
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
        "Say Thanks!": "https://saythanks.io/to/me%40mosquito.su",
    },
    packages=[],
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "Operating System :: MacOS",
        "Operating System :: POSIX",
        "Operating System :: Microsoft",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
    python_requires=">=3.5.*, <4",
    extras_require={
        "develop": [
            "pytest",
            "pytest-cov",
        ],
    },
)
