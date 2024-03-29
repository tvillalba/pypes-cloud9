"""
Custom component template
"""
from setuptools import setup, find_packages

setup(
    name = 'Cloud9',
    version = '0.1.0',
    description = '',
    author = '',
    author_email = '',
    url = '',
    packages=find_packages(),
    entry_points="""
        [pypesvds.plugins] 
        cloud9 = cloud9.cloud9:Cloud9

        [distutils.setup_keywords]
        paster_plugins = setuptools.dist:assert_string_list
  
        [egg_info.writers]
        paster_plugins.txt = setuptools.command.egg_info:write_arg
    """,
    paster_plugins = ['studio_plugin']
)

