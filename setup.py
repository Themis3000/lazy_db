#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

requirements = [ ]

test_requirements = [ ]

setup(
    author="Themi Megas",
    author_email='tcm4760@gmail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    description="A simple lazy loaded key:value database",
    install_requires=requirements,
    license="MIT license",
    long_description=readme,
    long_description_content_type='text/markdown',
    include_package_data=True,
    keywords='lazy_db',
    name='lazy_database',
    packages=find_packages(include=['lazy_db', 'lazy_db.*']),
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/Themis3000/lazy_db',
    version='0.1.1',
    zip_safe=False,
)
