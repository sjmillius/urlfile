import setuptools

setuptools.setup(
    name='urlfile',
    version='0.2.0',
    description='urlfile',
    long_description=open('README.md', 'r', encoding='utf8').read(),
    long_description_content_type="text/markdown",
    url='https://github.com/sjmillius/urlfile',
    project_urls={"Bug Tracker": "https://github.com/sjmillius/urlfile/issues"},
    license='Apache License 2.0',
    packages=['urlfile'],
    install_requires=[
        'cachetools',
        'requests',
    ])