"""PQ is a transactional queue for PostgreSQL."""

import sys
import os
from setuptools import setup, find_packages

setup(
    name='pq',
    version='1.0',
    url='https://github.com/malthe/pq/',
    license='BSD',
    author='Malthe Borch',
    author_email='mborch@gmail.com',
    description=__doc__,
    long_description='\n\n'.join((
        open('README.rst').read(),
        open('CHANGES.rst').read()
    )),
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    classifiers=[
        # As from http://pypi.python.org/pypi?%3Aaction=list_classifiers
        #'Development Status :: 1 - Planning',
        #'Development Status :: 2 - Pre-Alpha',
        #'Development Status :: 3 - Alpha',
        'Development Status :: 4 - Beta',
        #'Development Status :: 5 - Production/Stable',
        #'Development Status :: 6 - Mature',
        #'Development Status :: 7 - Inactive',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Internet',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Systems Administration',
    ],
    test_suite="pq.tests",
)
