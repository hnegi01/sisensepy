from setuptools import setup, find_packages

setup(
    name='pysisense',
    version='0.4',
    author='Himanshu Negi',
    author_email='himanshu.negi.08@gmail.com',
    description='A Python SDK for interacting with Sisense API',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/hnegi01/pysisense.git',
    packages=find_packages(),
    install_requires=[
        'requests',
        'pyyaml',
        'pandas'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
