import os 
import re
import requests
import numpy as np
from osgeo import osr, ogr, gdal

def parse_aoi(aoi = None, latlon = None):
    '''
    lalon can be coordinates but has to start with lat then lon
    you can pass 'lat,lon' as one string, but for multiple points
    searching the coordinates has to be 2d array like
    [[lat, lon],
    ...
     [lat, lon]]

    aoi can be geostrings of geometry(single) in WKT, JSON, WKB, GML
    aoi can be a raster or vector file
    aoi can be a list of them as well
    return:
        a list of cobinations of wkt gemetry or coordinates
    '''

    gdal.UseExceptions()
    ogr.UseExceptions() 
    wkts = []
    latlon = np.atleast_2d(latlon)
    for ll in latlon:
        ret = create_aoi_from_coords(ll)
        if ret is not None:
            wkts.append(ret)

    aoi = np.atleast_1d(aoi)
    #if isinstance(aoi,list) or isinstance(aoi, tuple) or isinstance(aoi, np.ndarray):
    for ao in aoi:
        geom = create_aoi_from_str(ao)
        if geom is not None:          
            wkt = geom.ExportToWkt()         
            wkts.append(wkt)
        else:
            wkts += create_aoi_from_file(ao)
        #if len(wkts)==0:
        #    raise IOError('AOI is not coordinates or general geometry string or raster or vector file....')
    return wkts

def create_aoi_from_str(aoi):
    g = None
    try:                 
        g = ogr.CreateGeometryFromJson(str(aoi))
    except:              
        try:             
            g = ogr.CreateGeometryFromGML(str(aoi))
        except:          
            try:         
                g = ogr.CreateGeometryFromWkt(str(aoi))
            except:      
                try:     
                    g = ogr.CreateGeometryFromWkb(str(aoi))                        
                except:
                    pass
    
    return g

def create_aoi_from_coords(aoi):
    # if its coords then it is 
    # should be able to convet to
    # array of float
    coords = re.findall(r"[-+]?\d*\.\d+|\d+", str(aoi))
    if len(coords) == 2:
        point = np.array(coords).astype(float).tolist()
        #point = ogr.Geometry(ogr.wkbPoint)
        # Create ring
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint(point[0] + 0.001, point[1] + 0.001)
        ring.AddPoint(point[0] + 0.001, point[1] - 0.001)
        ring.AddPoint(point[0] - 0.001, point[1] - 0.001)
        ring.AddPoint(point[0] - 0.001, point[1] + 0.001)
        ring.AddPoint(point[0] + 0.001, point[1] + 0.001)

        # Create polygon
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)

        #point.AddPoint(ret[0], ret[1])
        #poly = point.Buffer(0.0001) 
        ret = poly.ExportToWkt()
    else:
        ret = None
    return ret

def create_aoi_from_file(aoi):
    wkts = []
    if os.path.exists(str(aoi)):
        try:
            # raster file                                                                                                                     
            g = gdal.Open(aoi)
            subprocess.call(['gdaltindex', '-f', 'GeoJSON', '-t_srs', 'EPSG:4326', file_path + '/.aoi.json', aoi])
            dataSource = ogr.Open(file_path + '/.aoi.json')
            for layer in dataSource:  
                for feature in layer: 
                    geom = feature.GetGeometryRef()
                    wkt = geom.ExportToWkt()
                    wkts.append(wkt)
        except:     
            try:
                # vector file
                dataSource = ogr.Open(str(aoi))
                for layer in dataSource:   
                    for feature in layer:  
                        geom = feature.GetGeometryRef()
                        wkt = geom.ExportToWkt()
                        wkts.append(wkt)    
            except: 
                pass 
            pass
    return wkts
        
