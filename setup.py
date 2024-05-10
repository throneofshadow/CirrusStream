from setuptools import setup, find_packages

setup(
    name='cirrus_stream',
    version='0.0.1',
    install_requires=[
        'requests', 'docutils',
        'importlib-metadata; python_version>"3.11"',
    ],

    packages=find_packages(
        # All keyword arguments below are optional:
        where='cirrus_stream',  # '.' by default
        include=['cirrus_stream*'],  # ['*'] by default
        exclude=['cirrus_stream.tests'],  # empty by default
    ),
    include_package_data=True,
    # ...
)
