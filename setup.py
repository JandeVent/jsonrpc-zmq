# -*- coding: utf-8 -*-
# Copyright (C) 2018-2026 Connet Information Technology Company, Shanghai.

from setuptools import find_packages, setup, Extension
import os
import os.path as osp
import sys
import typing
import pathlib
from setup_config import PACKAGE_NAME, PROJECT_NAME, VERSION, EXTLIST, REQUIREMENTS_FILE

# =============================================================================
# Minimal Python version sanity check
# Taken from the notebook setup.py -- Modified BSD License
# =============================================================================
v = sys.version_info
if v[0] >= 3 and v[:2] < (3, 6):
    error = "ERROR: jsonrpc-zmq requires Python version 3.6 and above."
    print(error, file=sys.stderr)
    sys.exit(1)



def get_package_data(name, extlist):
    """
    Collect resource files from a package directory.

    This function recursively walks through the given package directory and
    gathers files that match the specified conditions. A match occurs when
    either the filename itself or its file extension appears in `extlist`.

    The function skips hidden files and any directory path that contains
    the substring "tests".

    Parameters
    ----------
    name : str
        Path to the package directory to search.

    extlist : Iterable[str]
        A list or iterable of matching conditions. Each element can be
        either a file extension (e.g. '.svg', '.png') or a specific
        filename (e.g. 'LICENSE'). Matching is case-insensitive.

    Returns
    -------
    list[str]
        A list of file paths relative to the package directory `name`.
    """
    files = []

    # Normalize all match patterns to lowercase for case-insensitive matching
    extlist_lower = {e.lower() for e in extlist}

    # Walk through the directory tree starting from `name`
    for dirpath, __dirnames, filenames in os.walk(name):

        # Skip directories that contain "tests" in their path
        if 'tests' in dirpath:
            continue

        for fname in filenames:
            # Skip hidden files (e.g. .gitignore)
            if fname.startswith('.'):
                continue

            # Extract the file extension in lowercase
            ext = osp.splitext(fname)[1].lower()

            # Check if the filename or extension matches the filter list
            if fname.lower() in extlist_lower or ext in extlist_lower:
                # Convert the file path to a path relative to `name`
                relpath = osp.relpath(osp.join(dirpath, fname), name)
                files.append(relpath)

    return files


def get_requirements() -> typing.List[str]:
    """Get installation requirements"""
    here = osp.abspath(osp.dirname(__file__))

    if pathlib.Path(REQUIREMENTS_FILE).is_file():
        with open(osp.join(here, REQUIREMENTS_FILE), 'r') as f:
            requirements = f.readlines()
        return requirements
    else:
        return []


setup(
    name=PROJECT_NAME,
    version=VERSION,
    packages=find_packages(),
    include_package_data=True,
    package_data={PACKAGE_NAME: get_package_data(PACKAGE_NAME, EXTLIST)},
    author="Connet Information Technology Company Ltd, Shanghai.",
    author_email="tech_support@shconnet.com.cn",
    description="JSONRPC ZMQ.",
    long_description="JSONRPC implementation using ZMQ.",
    long_description_content_type="text/markdown",
    keywords="JSONRPC ZMQ",
    url="",
    project_urls={
        "url": "https://github.com/JandeVent/jsonrpc-zmq"
    },
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Software Development :: Interpreters",
    ],
    install_requires=get_requirements(),
)