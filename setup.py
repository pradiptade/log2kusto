from setuptools import setup, find_packages


setup(
    name='log2kusto',
    version='0.1.0',
    description='Logging handler module that writes the logs to Kusto database.',
    long_description_content_type="text/markdown",
    long_description=open('README.md').read(),
    author='Pradipta De',
    author_email='pradipta.de@gmail.com',
    url='https://github.com/pradiptade/log2kusto',
    packages=find_packages(exclude=('tests'))
)

