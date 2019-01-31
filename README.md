### Feng Yin                          
### Department of Geography, UCL      
### ucfafyi@ucl.ac.uk                 
 
[![Build Status](https://travis-ci.org/MarcYin/S2_TOA_TO_LAI.svg?branch=master)](https://travis-ci.org/MarcYin/S2_TOA_TO_LAI)
 
Here, we use [SIAC](https://github.com/multiply-org/atmospheric_correction/) to do 
the atmospheric correction of Sentinel 2 TOA reflectance, then use inverse emulator 
to retrieve LAI from surface recflectance. These code will automatically download 
Sentinel 2 TOA reflectance data from [Copernicus Open Access Hub](https://scihub.copernicus.eu/) 
and do atmospheric correction with SIAC and give per pixel LAI value at 20 meters resolution.
 
 
## Requirements:                                                                                                                              
 
1. A NASA Earthdata username and password and can be applied [here](https://urs.earthdata.nasa.gov).
2. A Copernicus Open Access Hub username and password and can be applied [here](https://scihub.copernicus.eu/dhus/#/self-registration) 
 
## Installation:
 
1. Directly from github to get the most up to date version of it:                             
```bash                               
pip install https://github.com/MarcYin/S2_TOA_TO_LAI/archive/master.zip
```    
2. Using PyPI (This one is generally related to release)
```bash                               
pip install S2-TOA-TO-LAI
```  
3. Using anaconda from anaconda for 'better' package managements                               
```bash                               
conda install -c f0xy -c conda-forge s2-toa-to-lai
``` 

To save your time for installing GDAL:             
```bash
conda uninstall gdal libgdal
conda update --all -c conda-forge
conda install -c conda-forge gdal>2.1,<2.4
```  
 
## Usage
 
1. Using Sentinel 2 tiles directly:
```python
from S2_TOA_TO_LAI import TOA2LAI_S2
TOA2LAI_S2(tiles = ['50SMG'], start='2018-01-02', end='2018-01-03')
```
2. Using LatLon (Lat first then Lon) and this can be a 2D list of latlon:
```python
from S2_TOA_TO_LAI import TOA2LAI_S2
TOA2LAI_S2(latlon = '35.4, 56.2', start='2018-01-02', end='2018-01-03')
```
 
3. Using polygon from string(s) or (a) vector file(s):
```python                                            
from S2_TOA_TO_LAI import TOA2LAI_S2
aoi = 'POLYGON((115.79984234354565 39.41267418434987,115.81853363330639 39.41267418434987,115.81853363330639 39.42542974293974,115.79984234354565 39.42542974293974,115.79984234354565 39.41267418434987))' # or a vector file 
TOA2LAI_S2(aoi = aoi, start='2018-01-02', end='2018-01-03')
``` 
 
*You can also specify `cloud_cover` but this may lead to losing of S2 observations due to a bad cloud mask from L1C data*

