from setuptools import setup, find_packages

def read_requirements():
    with open('requirements.txt', 'r') as req_file:
        return req_file.read().splitlines()

url = ""
author = ""
author_email = ""

setup(
    name='transformslib',
    version='0.0.4',
    author=author,
    author_email=author_email,
    description='A python package of a working transforms framework',
    long_description=open('readme.md').read(),
    long_description_content_type='text/markdown',
    url=url,  
    packages=["transformslib"],
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.10',
    install_requires=read_requirements()
)