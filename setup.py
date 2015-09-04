"""
An asynchronous peewee database adapter for psycopg using momoko.
"""

from setuptools import setup

__version__ = '0.1.0'

setup(
    name='peewee-momoko',
    version=__version__,
    author='Michael Lavers',
    author_email='kolanos@gmail.com',
    url='https://github.com/txtadvice/peewee-momoko',
    description=__doc__,
    # long_description=__doc__,
    license='MIT',
    install_requires=(
        'peewee>=2.6.3',
        'momoko>=2.1.0',
    ),
    py_modules=[
        'peewee_momoko',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ]
)