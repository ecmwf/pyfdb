from setuptools import setup, find_packages
setup(
    name='pyfdb',
    install_requires=['cffi'],
    packages = find_packages(),
    include_package_data = True,
    description = 'Python interface to FDB',
    url = 'https://git.ecmwf.int/projects/MARS/repos/pyfdb/browse',
    author = 'ECMWF',
    author_email = 'software.support@ecmwf.int',
)
