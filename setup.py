#!/usr/bin/env python
# -*- coding: utf-8 -*-
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import ellison

setup(
    name=ellison.__title__,
    version=ellison.__version__,
    description="Object oriented mapping for Python and Mongo DB.",
    long_description=open("README.md").read(),
    author="Roman Kalyakin",
    author_email="roman@kalyakin.com",
    url='http://github.com/theorm/ellison/',
    license='Apache License, Version 2.0',
    keywords="mongo mongodb pymongo oop mapping",
    packages=['ellison'],
    package_data={'': ['LICENSE', 'README.md']},
    include_package_data=True,
    install_requires=['pymongo'],
    tests_require=["nose"],
    test_suite="tests",
    classifiers=(
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules'
    )
)
