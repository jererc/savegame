from setuptools import setup, find_packages

setup(
    name='savegame',
    version='2025.09.01.180131',
    author='jererc',
    author_email='jererc@gmail.com',
    url='https://github.com/jererc/savegame',
    packages=find_packages(exclude=['tests*']),
    python_requires='>=3.10',
    install_requires=[
        'dateutils',
        'google-api-python-client',
        'google-auth-httplib2',
        'google-auth-oauthlib',
        # 'svcutils @ git+https://github.com/jererc/svcutils.git@main#egg=svcutils',
        'svcutils @ https://github.com/jererc/svcutils/archive/refs/heads/main.zip',
        # 'goth @ git+https://github.com/jererc/goth.git@main#egg=goth',
        'goth @ https://github.com/jererc/goth/archive/refs/heads/main.zip',
    ],
    extras_require={
        'dev': ['flake8', 'pytest'],
    },
    entry_points={
        'console_scripts': [
            'savegame=savegame.main:main',
        ],
    },
    include_package_data=True,
)
