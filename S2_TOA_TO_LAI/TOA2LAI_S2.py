import os 
import gdal
import json
import warnings
warnings.filterwarnings("ignore") 
import subprocess
import numpy as np
from glob import glob
from SIAC import SIAC_S2
from .nnModel import predict
from .query import getS2Files
from datetime import datetime
from .create_logger import create_logger

home = os.path.expanduser("~")
file_path = os.path.dirname(os.path.realpath(__file__))
logger = create_logger()
logger.propagate = True
now = datetime.now().strftime('%Y-%m-%d')

def TOA2LAI_S2(aoi = None, 
               tiles = None, 
               latlon = None, 
               start='2020-06-01', 
               end=now, 
               cloudCover= 34.5, 
               producttype = 'S2MSI1C',   
               orderby = 'beginposition',
               sort = 'desc',
               valPixThresh = 0,
               s2FileDir = './',
               jasmin = False,
               mcd43  = '~/MCD43/',
               vrt_dir = '~/MCD43_VRT/',
               processingAOI = None,
              ):

    s2TileDirs = getS2Files(aoi = aoi, 
                             tiles = tiles, 
                             latlon = latlon, 
                             start = start, 
                             end = end, 
                             cloudCover = cloudCover, 
                             producttype = producttype,   
                             orderby = orderby,
                             sort = sort,
                             valPixThresh = valPixThresh,
                             searchStr = None,
                             s2FileDir = s2FileDir
                             )
    for s2TileDir in s2TileDirs:
         AC_LAI(s2TileDir, mcd43 = mcd43, vrt_dir = vrt_dir, jasmin=jasmin, aoi=processingAOI)

    return s2TileDirs
 
def AC_LAI(fname, mcd43 = '~/MCD43/',vrt_dir = '~/MCD43_VRT/', jasmin=False, aoi = None, ):
    '''
    jasmin is only for UK jasmin computing platform
    '''
    siacJson = os.path.join(fname, 'siac_output.json')
    if not os.path.exists(siacJson):
        SIAC_S2(fname, 
                send_back = False, 
                mcd43     = mcd43 , 
                vrt_dir   = vrt_dir, 
                jasmin    = jasmin,
                aoi       = aoi, 
               ) 
    else:
        logger.info('Atmospheric correction has been done for %s'%(os.path.basename(fname)))
    
    with open(siacJson, 'r') as f:
        siacOutput = json.load(f)
    surRefs = siacOutput['features'][0]['SurfaceReflectance']
    
    viewAngs= siacOutput['features'][0]['viewAngles'][3]
    sunAngs = siacOutput['features'][0]['sunAngles']
    
    cloud = siacOutput['features'][0]['cloud']
    
    bNames = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B09', 'B10', 'B11', 'B12']
    inds = [1, 2, 3, 4, 5, 6, 8, 11, 12]
    
    requiredBands = [surRefs[i] for i in inds]
    nnInputs = requiredBands + [viewAngs, sunAngs, cloud]
    res = [10, 10, 10, 20, 20, 20, 20, 20, 20, 10, 500, 60]
    nnInputGs = []
    logger.info('Resamplling data to 10 meters spatial resolution.')
    for i in range(len(nnInputs)):
        inp = os.path.join(os.path.dirname(fname), nnInputs[i])
        if res[i] != 10:
            g = gdal.Warp('', inp, format = 'MEM', xRes=10, yRes=10, resampleAlg=1)
        else:
            g = gdal.Open(inp)
            arrShape = g.RasterYSize, g.RasterXSize
        nnInputGs.append(g)
    numStripes = 20
    
    stripes = np.linspace(0, arrShape[0], numStripes + 1).astype(int)
    
    nnModelFile = file_path + '/data/nnLai.npz'
    f = np.load(nnModelFile, allow_pickle=True)
    modelArrays = f.f.mdoel
    outputs = []
    for i in range(numStripes):
        logger.info('Doing prediction on stripe %d of %d'%(i + 1, numStripes))
        start = stripes[i]
        end   = stripes[i + 1]
        nnInputs = []
        for nnInputG in nnInputGs:
            data = nnInputG.ReadAsArray(int(0), int(start), int(arrShape[1]), int(end - start))
            nnInputs.append(data)

        # T means transformed for NN input
        Tvza = np.cos(np.deg2rad(nnInputs[-3][1] / 100.))
        Tsza = np.cos(np.deg2rad(nnInputs[-2][1]  / 100.))
        Traa = nnInputs[-3][0] - nnInputs[-2][0]
        
        Traa[Traa<0] = Traa[Traa<0] + 360
        Traa = (Traa / 100.) / 360.
        
        surs = np.array(nnInputs[:-3])
        
        mask = (nnInputs[-1] < 40) & (np.all(surs > 0, axis=0))
        
        nnInput = np.vstack([surs / 10000, [Tsza, Tvza, Traa]])
        inputs = nnInput[:, mask].astype(np.float32)
        output = np.ones_like(mask) * -9999
        if inputs.size > 0:
            out = predict(inputs.T, modelArrays).ravel()
            output[mask] = np.log(out) * -2 * 1000
        outputs.append(output.astype(int))
    outputs = np.vstack(outputs)
    
    laiFname = save_lai(outputs, os.path.join(os.path.dirname(fname), surRefs[1]))
    siacOutput['features'][0]['lai'] = laiFname.replace(os.path.dirname(fname), '')[1:] # skip '/' or '\' in the fname
    with open(siacJson, 'w') as f:
        json.dump(siacOutput, f, ensure_ascii=False, indent=4)
    return siacOutput

def save_lai(array, exampleFile):
    logger.info('Saving LAI into file.')
    g = gdal.Open(exampleFile)
    toa_dir = os.path.dirname(exampleFile)
    xmin, ymax   = g.GetGeoTransform()[0], g.GetGeoTransform()[3]
    projection   = g.GetProjection()
    geotransform = (xmin, 10, 0, ymax, 0, -10)
    nx, ny       = array.shape
    outputFileName = exampleFile.replace('B02_sur.tif', 'lai.tif')
    if os.path.exists(outputFileName):
        os.remove(outputFileName)
    dst_ds = gdal.GetDriverByName('GTiff').Create(outputFileName, ny, nx, 1, gdal.GDT_Int16, options=["TILED=YES", "COMPRESS=DEFLATE"])
    dst_ds.SetGeoTransform(geotransform)
    dst_ds.SetProjection(projection)
    dst_ds.GetRasterBand(1).WriteArray(array)
    dst_ds.FlushCache()
    dst_ds = None    
    return outputFileName

def test():
    print('Test S2 TOA to LAI\n')
    ao = 'POLYGON((115.79984234354565 39.41267418434987,' + \
              '115.81853363330639 39.41267418434987,' + \
              '115.81853363330639 39.42542974293974,' + \
              '115.79984234354565 39.42542974293974,' + \
              '115.79984234354565 39.41267418434987))'
    ret = TOA2LAI_S2(tiles='50SMG', aoi = ao, start='2019-06-01', end ='2019-06-15', cloudCover=20, jasmin = True)
    return ret
    print('Done!\n')
if __name__ == '__main__':
    ret = test()