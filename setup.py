from setuptools import setup, find_packages

setup(
    name='baus',
    version='0.1dev',
    description='Bay Area UrbanSim.',
    author='Metropolitan Transportation Commission',
    license='BSD',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: BSD License'
    ],
    packages=find_packages(exclude=['*.tests']),
    install_requires=[
        'boto'
    ]
)
