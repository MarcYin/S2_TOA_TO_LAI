import os 
import sys
import zipfile
import requests
import warnings
warnings.filterwarnings("ignore") 
import subprocess
import numpy as np
import gp_emulator
from glob import glob
from SIAC import SIAC_S2
from threading import Timer
from datetime import datetime
from functools import partial
from os.path import expanduser
from osgeo import osr, ogr, gdal
from subprocess import Popen, PIPE
import S2_TOA_TO_LAI.get_scihub_pass
from S2_TOA_TO_LAI.query import query_sen2
from multiprocessing import Pool, cpu_count
from S2_TOA_TO_LAI.parse_aoi import parse_aoi
from S2_TOA_TO_LAI.create_logger import create_logger

home = expanduser("~")
file_path = os.path.dirname(os.path.realpath(__file__))
logger = create_logger()
SIAC_S2_file = SIAC_S2.__globals__['__file__']

username, password = np.loadtxt(file_path + '/.scihub_auth', dtype=str)
auth = tuple([username, password])


now = datetime.now().strftime('%Y-%m-%d')
def find_S2_files(aoi = '', tiles = '', latlon = None, start = '2015-12-01', end = now, cloud_cover = 100, one_by_one=True, band = None):
    wkts = []
    rets = []
    if len(aoi) > 0:
        wkts += parse_aoi(aoi = aoi)
    if latlon is not None:
        wkts += parse_aoi(latlon = latlon)
    for wkt in wkts:
        rets += query_sen2(wkt, search_by_tile=True, start=start, end=end, cloud_cover=100, band=None, one_by_one=True) 

    if len(tiles) > 0:
        tiles = np.atleast_1d(tiles).tolist()
        for tile in tiles:
            rets += query_sen2('', search_by_tile=tile, start=start, end=end, cloud_cover=100, band=None, one_by_one=True)
    return rets

def down_s2_file(rets, s2_file_dir = home + '/S2_data'):
    url_fnames = [[j, s2_file_dir + '/' + i] for [i, j] in rets]
    p = Pool(2)
    ret = p.map(downloader, url_fnames)

def downloader(url_fname):
    url, fname = url_fname
    logger.info('Try to download %s'%fname.split('/')[-1])
    if os.path.exists(fname + '.SAFE'):
        logger.info('%s exists, skip downloading'%fname.split('/')[-1])
    else:
        r  = requests.get(url, stream = False, headers={'user-agent': 'My app'}, auth = auth)
        remote_size = int(r.headers['Content-Length'])
        if r.ok:
            data = r.content
            if len(data) == remote_size:
                with open(fname, 'wb') as f:
                    f.write(data)
            else:           
                raise IOError('Failed to download the whole file.')
        else: 
            logger.error(r.content)

def TOA2LAI_S2(aoi = '', tiles = '', latlon = None, start = '2015-12-01', end = now, cloud_cover = 100, s2_file_dir = home + '/S2_data'):
    #logger.propagate = False
    rets = find_S2_files(aoi = aoi, tiles = tiles, latlon = latlon, start = start, end =end, cloud_cover = cloud_cover, one_by_one=True, band = None)
    if os.path.realpath(s2_file_dir) in os.path.realpath(home + '/S2_data'):
        if not os.path.exists(home + '/S2_data'):
            os.mkdir(home + '/S2_data') 
    logger.info('Start downloading all the files..')
    down_s2_file(rets, s2_file_dir = s2_file_dir)
    fnames = [s2_file_dir + '/' + i[0]  for i in rets]
    for fname in fnames:
        if not os.path.exists(fname + '.SAFE'):
            logger.info('Start unziping..')
            logger.info('Unziping %s' %fname.split('/')[-1])
            zip_ref = zipfile.ZipFile(fname, 'r')
            zip_ref.extractall(s2_file_dir)
            zip_ref.close()
            os.remove(fname)
    fnames =  [s2_file_dir + '/' + i[0] + '.SAFE' for i in rets]
    corrected = do_ac(fnames)
    lais = do_lai(corrected)
    return [corrected, lais]

def do_lai(fnames):
    logger.info('Doing LAI..')
    f = np.load(file_path + '/data/Third_comps.npz')
    comps = f.f.comps
    AAT  = np.dot(comps.T, comps)
    gp =  gp_emulator.GaussianProcess(emulator_file=file_path + '/data/Third_inv_gp.npz')
    lais = []
    for fname in fnames:
        if (len(glob(fname+'/GRANULE/*/IMG_DATA/lai.tif')) == 0) & (len(glob(fname+'/GRANULE/*/IMG_DATA/BOA_RGB.tif')) > 0):
            boa = read_boa(fname)
            lai = pred_lai(boa, comps, gp, AAT)
            lai_name = save_lai(lai, fname)
            lais.append(lai_name)
        elif len(glob(fname+'/GRANULE/*/IMG_DATA/BOA_RGB.tif')) == 0:
            logger.error('Atmospheric correction has not been done for %s, so no LAI inversion.'%fname.split('/')[-1])
        elif len(glob(fname+'/GRANULE/*/IMG_DATA/lai.tif')) > 0:
            logger.info('%s LAI inversion has been done and skipped.'%fname.split('/')[-1])
            lai_name = glob(fname+'/GRANULE/*/IMG_DATA/lai.tif')[0]
            lais.append(lai_name)
    logger.info('Done!')
    return lais

def save_lai(array, fname):
    logger.info('Saving LAI into file.')
    b2 = glob(fname + '/GRANULE/*/IMG_DATA/*_B02_sur.tif')[0]
    g = gdal.Open(b2)
    toa_dir = '/'.join(b2.split('/')[:-1])
    xmin, ymax   = g.GetGeoTransform()[0], g.GetGeoTransform()[3]
    projection   = g.GetProjection()
    geotransform = (xmin, 20, 0, ymax, 0, -20)
    nx, ny       = array.shape
    outputFileName = toa_dir + '/lai.tif'
    if os.path.exists(outputFileName):
        os.remove(outputFileName)
    dst_ds = gdal.GetDriverByName('GTiff').Create(outputFileName, ny, nx, 1, gdal.GDT_Float32, options=["TILED=YES", "COMPRESS=DEFLATE"])
    dst_ds.SetGeoTransform(geotransform)
    dst_ds.SetProjection(projection)
    dst_ds.GetRasterBand(1).WriteArray(array)
    dst_ds.FlushCache()
    dst_ds = None    
    return outputFileName

def read_boa(fname):
    logger.info('Read in surface reflectance.')
    boa = []    
    used_bands = ['B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B8A']
    cloud = glob(fname + '/GRANULE/*/cloud.tif')[0]
    fdir = cloud.split('cloud.tif')[0] + 'IMG_DATA/'
    mask = gdal.Warp('', cloud, warpOptions = ['NUM_THREADS=ALL_CPUS'], \
                  cutlineDSName = fdir + '/AOI.json', cropToCutline = True, \
                  xRes=20, yRes=20, resampleAlg=0, format = 'MEM').ReadAsArray()
    mask = mask > 60
    b2 = glob(fname + '/GRANULE/*/IMG_DATA/*_B02_sur.tif')[0]
    for band in used_bands:
        logger.info('Read in %s.' % band)
        data = gdal.Warp('', b2.replace('_B02_sur.tif', '_%s_sur.tif'%band), \
                  cutlineDSName = fdir + '/AOI.json', cropToCutline = True,warpOptions = ['NUM_THREADS=ALL_CPUS'], \
                  xRes=20, yRes=20, resampleAlg=0, format = 'MEM').ReadAsArray() / 10000.
        data[(data<0) | mask] = np.nan
        boa.append(data)
    boa = np.array(boa) 
    return boa

def pred_lai(boa, comps, gp, AAT):
    logger.info('Doing LAI prediction.')
    mask = np.any(np.isnan(boa), axis=(0))
    val_vals = boa[:, ~mask]    
    lai = []
    num_threads = np.minimum(12, cpu_count())
    for val_val in np.array_split(val_vals, 20, axis=1):
        p = Pool(num_threads)
        par = partial(do_pred, comps = comps, gp = gp, AAT=AAT)
        ret = p.map(par, np.array_split(val_val, num_threads, axis=1))
        for i in ret:
            lai +=i.tolist()
    lai_map = np.copy(boa[0])
    lai_map[~mask] = lai
    return lai_map


def do_pred(val_val, comps, gp, AAT):
    ATy  = np.dot(comps.T, val_val)  
    x    = np.linalg.solve(AAT, ATy) 
    plai = gp.predict(x[1:-1].T)[0]  
    lai = -2 * np.log(plai) 
    return lai


def do_ac(fnames):
    corrected = []
    for fname in fnames:
        logger.info('Doing atmospheric correction for %s.'%fname.split('/')[-1])
        if len(glob(fname+'/GRANULE/*/IMG_DATA/BOA_RGB.tif')) ==0:
            cmd = ['python', SIAC_S2_file, '-f', fname]
            if sys.version_info >= (3,0):
                run_ac_timer_py3(cmd, 3600, fname)
            else:
                run_ac_timer_py2(cmd, 3600, fname)
            if len(glob(fname+'/GRANULE/*/IMG_DATA/BOA_RGB.tif')) > 0:
                corrected.append(fname)
            else:
                logger.error('Atmospheric correction has failed for %s'%fname.split('/')[-1])
        else:
            logger.info('%s has been corrected and skipped.'%fname.split('/')[-1])
            corrected.append(fname)
    return corrected

def run_ac_timer_py3(cmd, timeout_sec, fname):
    try:                       
        subprocess.run(cmd, timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        logger.error('%s ran too long and is killed'%fname.split('/')[-1])
    except:                    
        logger.error('%s errored during processing'%fname.split('/')[-1])

def run_ac_timer_py2(cmd, timeout_sec, fname):
    proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
    timer = Timer(timeout_sec, proc.kill)
    try:
        timer.start()
        stdout, stderr = proc.communicate()
    finally:
        logger.error('%s errored during processing'%fname.split('/')[-1])
        timer.cancel()
