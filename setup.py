from setuptools import setup

long_description = None
with open("README.md", 'r') as fp:
    long_description = fp.read()

setup(
    name = 'graphql_subscription_manager',
    packages = ['graphql_subscription_manager'],
    install_requires=['websockets>=6.0'],
    version='0.3.6',
    description='A python3 library for graphql subscription manager',
    long_description=long_description,
    python_requires='>=3.5.3',
    author='Daniel Hoyer Iversen',
    author_email='mail@dahoiv.net',
    url='https://github.com/Danielhiversen/PyGraphqlWebsocketManager',
    license="MIT",
    classifiers=[
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Topic :: Home Automation',
        'Topic :: Software Development :: Libraries :: Python Modules'
        ]
)
