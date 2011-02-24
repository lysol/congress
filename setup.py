#!/usr/bin/env python

from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

setup(name='congress',
      version='0.1.0',
      cmdclass = {"build_ext": build_ext},
      description='Kademlia-like DHT Implementation',
      author='Derek Arnold',
      author_email='derek@brainindustries.com',
      url='http://lysol.github.com/congress',
      packages=['congress'],
      package_dir={'congress': 'src/congress'},
      ext_modules=[Extension("_congress", ["src/congress/_congress.pyx"])],
      license='BSD',
      classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Unix",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Distributed Computing",
        "Programming Language :: Python :: 2.7"
      ],
      download_url="https://github.com/lysol/congress/tarball/master",
      requires=(
        'pyev',
        'dogfood'
        )
     )
