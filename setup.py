from setuptools import setup, find_packages
from codecs import open
from os import path

__version__ = '0.1.0'

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

install_requires = ['aiohttp']
dependency_links = []

setup(
    name='qcloud_cos_py3',
    version=__version__,
    description='A 3rd-party SDK for Qcloud COS and Python 3',
    long_description=long_description,
    url='https://github.com/su27/qcloud_cos_py3',
    download_url='https://github.com/su27/qcloud_cos_py3/tarball/' + __version__,
    license='BSD',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
    ],
    keywords='qcloud',
    packages=find_packages(exclude=['docs', 'tests*']),
    include_package_data=True,
    author='Dan Su',
    install_requires=install_requires,
    dependency_links=dependency_links,
    author_email='damn.su@gmail.com'
)
