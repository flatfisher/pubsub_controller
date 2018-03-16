# -*- coding:utf-8 -*-
import re
from setuptools import setup, find_packages

try:
    with open('README.rst') as f:
        readme = f.read()
except IOError:
    readme = ''

try:
    with open('LICENSE') as f:
        license = f.read()
except IOError:
    license = ''

with open('apps/__init__.py', 'r') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
            fd.read(), re.MULTILINE).group(1)

print('This package will fetch one subscription and pull if there is a message.')
print('If you have not created Pub/Sub\'s topic and subscription on the GCP Console yet, please create it.')
GCP_PROJECT_ID = raw_input('Please input GCP_PROJECT_ID >>>  ')
SUBSCRIPTION_ID = raw_input('Please input SUBSCRIPTION_ID >>>  ')

with open('settings.py', 'a') as fd:
    fd.write('GCP_PROJECT_ID = \'{}\'\n'.format(GCP_PROJECT_ID))
    fd.write('SUBSCRIPTION_ID = \'{}\'\n'.format(SUBSCRIPTION_ID))

setup(
    name='pubsub_controller',
    version=version,
    description='It provides a resident process that periodically pull subscribes and publish on CLI and Python.',
    long_description=readme,
    author='SENSY Inc.',
    url='https://github.com/COLORFULBOARD/pubsub_controller',
    license=license,
    install_requires=[
        'google-cloud-pubsub==0.30.1'
    ],
    packages=find_packages()
)

print('Setup Completed.')