from distutils.core import setup

# read the contents of your README file
from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
  name = 'snowpark_utilities',         # How you named your package folder (MyLib)
  packages = ['snowpark_utilities'],   # Chose the same as "name"
  version = '0.1.1',      # Start with a small number and increase it with every change you make
  license='MIT',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'A helpful package for making snowpark code easier to write and more legible',   # Give a short description about your library
  long_description=long_description,
  long_description_content_type='text/markdown',
  author = 'Theodore Caulton',                   # Type in your name
  author_email = 'teddycaulton@live.com',      # Type in your E-Mail
  url = 'https://teddycaulton.xyz',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/teddycaulton/Snowpark-Utilities/archive/refs/tags/v_011.tar.gz',    # I explain this later on
  keywords = ['Snowpark', 'Snowflake', 'Data Science'],   # Keywords that define your package best
  install_requires=[            # I get to this in a second
          'boto3',
          'snowflake-snowpark-python',
          'pandas'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   # Again, pick a license
    'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
)