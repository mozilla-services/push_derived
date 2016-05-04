from setuptools import setup

setup(
    name='push_derived',
    version='0.1',
    py_modules=['analytics'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        push_metrics=analytics.main_cli:cli
    ''',
)
