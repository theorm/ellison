# -*- coding: utf-8 -*-
from setuptools import setup

setup(
    name='ellison',
    version='0.1',
    url='http://github.com/theorm/ellison/',
    license='Apache License, Version 2.0',
    author="Roman Kalyakin",
    author_email="roman@kalyakin.com",
    description="Object oriented mapping for Python and Mongo DB.",
    long_description=open("README.md").read(),
    keywords=["mongo", "mongodb", "pymongo", "oop", "mapping"],
    install_requires=['pymongo'],
    tests_require=["nose"],
    test_suite="tests",
    py_modules=['ellison'],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
