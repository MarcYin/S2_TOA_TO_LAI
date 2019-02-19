import os                                             
from setuptools import setup                          
file_path = os.path.dirname(os.path.realpath(__file__))
                                                      
try:                                                  
    version = os.environ['S2_TOA_TO_LAI_VERSION']              
except:                                               
    version_file = open(os.path.join(file_path, 'S2_TOA_TO_LAI/VERSION'), 'rb')
    version = version_file.read().decode().strip()   
                    
with open('README.md', 'rb') as f:
    readme = f.read().decode()
                    
setup(name                          = 'S2_TOA_TO_LAI',
      version                       = version,
      description                   = 'From Sentinel 2 TOA to LAI',
      long_description              = readme,
      long_description_content_type ='text/markdown',
      author                       = 'Feng Yin',
      author_email                 = 'ucfafyi@ucl.ac.uk',
      classifiers                  = ['Development Status :: 4 - Beta',
                                      'Programming Language :: Python :: 2.7',
                                      'Programming Language :: Python :: 3.6'],
      install_requires             = ['siac', 'gdal>=2.1', 'requests', 'gp_emulator', 'numpy>=1.13', 'scipy>=1.0'],
      url                          = 'https://github.com/MarcYin/S2_TOA_TO_LAI',
      license                      = "GNU Affero General Public License v3.0",
      include_package_data         = True,
      packages                     = ['S2_TOA_TO_LAI'],
     )  
