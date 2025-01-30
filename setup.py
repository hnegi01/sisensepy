from setuptools import setup, find_packages

setup(
    name='sisensepy',
    version='0.1.0',
    license="MIT",
    author='Himanshu Negi',
    author_email='himanshu.negi.08@gmail.com',
    description='A Python SDK for interacting with Sisense API',
    long_description=open('README.md', encoding="utf-8").read(),
    long_description_content_type='text/markdown',
    url='https://github.com/hnegi01/sisensepy', 
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
    python_requires='>=3.9',
)
