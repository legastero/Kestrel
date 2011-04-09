import sys, os
from distutils.core import setup

import kestrel

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(name='kestrel',
      version=kestrel.__version__,
      description='An XMPP-based Many-Task Computing Framework',
      long_description=read('README'),
      classifiers=['Development Status :: 3 - Alpha',
                   'Environment :: Console',
                   'Intended Audience :: Science/Research',
                   'Intended Audience :: Education',
                   'License :: OSI Approved :: Apache Software License',
                   'Operating System :: OS Independent',
                   'Topic :: System :: Distributed Computing',
                   'Programming Language :: Python'],
      keywords='xmpp sleekxmpp cloud grid',
      author='Lance Stout',
      author_email='lancestout@gmail.com',
      url='http://github.com/legastero/kestrel',
      license='Apache License 2.0',
      packages=['kestrel', 'kestrel/plugins', 'kestrel/plugins/kestrel_manager'],
      scripts=['scripts/kestrel'],
      include_package_data=True,
      zip_safe=False,
      install_requires=['sleekxmpp'])
