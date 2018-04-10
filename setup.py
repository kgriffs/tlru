import imp
from os import path

from setuptools import find_packages, setup

VERSION = imp.load_source('version', path.join('.', 'tlru', 'version.py'))
VERSION = VERSION.__version__

REQUIRES = [
    'backoff',
    'csiphash',
    'msgpack',
    'pecyn',

    # NOTE(kgriffs): Be sure to "export XXHASH_FORCE_CFFI=1" before installing
    #   under PyPy!
    'xxhash',
]


def _load_description():
    with open('README.md', 'r', encoding='utf-8') as f:
        return f.read()


setup(
    name='tlru',
    version=VERSION,
    url='https://github.com/falconry/tlru',
    description='Time-bound LRU caching library',
    long_description=_load_description(),
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Natural Language :: English',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    author='Kurt Griffiths',
    author_email='mail@kgriffs.com',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    setup_requires=['setuptools>=38.6.0'],
    install_requires=REQUIRES,
    tests_require=['pytest', 'coverage'],
    python_requires='>=3.5',
)
