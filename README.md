# From Sentinel 2 TOA reflectance to LAI
### Feng Yin                          
### Department of Geography, UCL      
### ucfafyi@ucl.ac.uk                 

[![Build Status](https://travis-ci.org/MarcYin/S2_TOA_TO_LAI.svg?branch=master)](https://travis-ci.org/MarcYin/S2_TOA_TO_LAI)

Here, we use [SIAC](https://github.com/multiply-org/atmospheric_correction/) to do 
the atmospheric correction of Sentinel 2 TOA reflectance, then use inverse emulator 
to retrieve LAI from surface recflectance. These code will automatically download 
Sentinel 2 TOA reflectance data from [Copernicus Open Access Hub](https://scihub.copernicus.eu/) 
and do atmospheric correction with SIAC and give per pixel LAI value at 20 meters.
