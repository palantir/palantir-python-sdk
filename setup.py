#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.

import os

from setuptools import find_packages, setup

setup(
    # Environment variables set by conda build
    name="foundry-sdk",
    version=os.environ.get("PKG_VERSION"),
    python_requires=">=3.8.0",
    description="Foundry Python SDK",
    # The project's main homepage.
    url="https://github.com/foundry/api-gateway",
    author="Palantir Technologies, Inc.",
    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=["docs*", "test*", "integration*"]),
    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=["requests"],
    # List additional groups of dependencies here (e.g. development
    # dependencies). You can install these using the following syntax,
    # for example:
    # $ pip install -e .[test]
    extras_require={},
)
