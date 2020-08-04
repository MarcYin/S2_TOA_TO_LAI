import os
import fnmatch
import zipfile
import requests
import subprocess
import numpy as np
from osgeo import ogr
from osgeo import osr
from functools import partial
from .parse_aoi import parse_aoi
from multiprocessing import Pool
from .create_logger import create_logger
from xml.etree import cElementTree as ET
from .get_scihub_pass import loginScihub
from datetime import datetime, timedelta
from collections import defaultdict, namedtuple

file_path = os.path.dirname(os.path.realpath(__file__))
logger = create_logger()

username, password = np.loadtxt(file_path + '/.scihub_auth', dtype=str)
auth = tuple([username, password])

source = osr.SpatialReference()
source.ImportFromEPSG(4326)
#This is important to keep lon, lat rather than lat, lon
source.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER) 
target = osr.SpatialReference()
target.ImportFromProj4('+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs')
transform = osr.CoordinateTransformation(source, target)

tile_area = 12064298699.657892
now = datetime.now().strftime('%Y-%m-%d')

base = 'https://scihub.copernicus.eu/dhus/search?start=0&rows=100&q='

def parse_feed(feed, valPixThresh):                                               
    ret = []
    total = int(feed['opensearch:totalResults'])
    if total > 0:
        if total == 1:
            feed['entry'] = [feed['entry']]
        for i in range(len(feed['entry'])):                                     
            title = feed['entry'][i]['title']                                   
            cloud = feed['entry'][i]['double']['content']                       
            durl  = feed['entry'][i]['link'][0]['href']
            qurl  = feed['entry'][i]['link'][2]['href']
            for ds in feed['entry'][i]['date']:
                if ds['name'] == u'beginposition':
                    date  = ds['content'] 
            for j in feed['entry'][i]['str']:
                if 'POLYGON ((' in j['content']:
                    foot = j['content']
                    geom = ogr.CreateGeometryFromWkt(foot)
                    geom.Transform(transform)
                    val_pix = min((geom.GetArea()/tile_area)*100., 100.)
                    if val_pix >= valPixThresh:
                        ret.append([title, date, foot, float(cloud), [durl], qurl, val_pix])
    return ret

def defineCloudCover(cloudCover):
    if isinstance(cloudCover, (int, float)):
        cloudCover = '[0 TO %s]'%cloudCover
    elif isinstance(cloudCover, str):
        cloudCover = cloudCover
    else:
        raise IOError('Cloud cover can only be number or string, e.g. "[0 TO 10.1]"')
    return '( cloudcoverpercentage:%s )' % (cloudCover)

def defineFname(tile):
    if tile is not None:
        fname = '( filename:S2?_MSIL1C_????????T??????_N*_R*_T%s* )'%tile
    else:
        fname = '( filename:S2?_MSIL1C_????????T??????_N*_R*_T* )'
    return fname

def defineFootprint(wkt):
    if wkt is not None:
        footprint = '( footprint:" Intersects(%s)" )'%wkt
    else:
        footprint = None
    return footprint

def defineTime(start, end):
    beginPosition = '( beginPosition:[%sT00:00:00.000Z TO %sT23:59:59.999Z] ' % (start, end)
    endPosition   = 'AND endPosition:[%sT00:00:00.000Z TO %sT23:59:59.999Z] )' % (start, end)
    return beginPosition + endPosition

def definePlatformProducttype(platformname, producttype):
    return '( platformname:%s AND producttype:%s )' % (platformname, producttype)

def defineTail(orderby, sort):
    return '&orderby=%s %s& &format=json' % (orderby, sort)

def searchPage(page, pages, url, auth, valPixThresh):
    logger.info ('searching page %d of %d'%(page+1, pages))   
    url = url.replace('start=0&rows=100', 'start=%d&rows=100'%(page*100))
    r = requests.get(url, auth = auth)
    if r.ok:
        feed = r.json()['feed']                                                 
        ret  = parse_feed(feed, valPixThresh)
    else:
        ret = []
        logger.info(r.content)
    return ret

def searchBand(entry, searchStr, auth):
    url = "%s/Nodes('%s.SAFE')/"%(entry[4][0].split('/$value')[0], entry[0])
    r = requests.get(url + "Nodes('manifest.safe')/$value" , auth = auth)
    if r.ok:
        e = ET.XML(r.content)                        
        d = etree_to_dict(e)                         
        dataObject = d['{urn:ccsds:schema:xfdu:1}XFDU']['dataObjectSection']['dataObject']
        fnames = [do['byteStream']['fileLocation']['href'] for do in dataObject]
        urls = []
        fnames = fnmatch.filter(fnames, searchStr)
        for fname in fnames:
            furl = fname.replace('/', "')/Nodes('").replace(".')/", url) + "')/$value"
            urls.append(furl)
    else:
        urls = []
        logger.info(r.content)
    return urls

def etree_to_dict(t):
    # from https://stackoverflow.com/questions/7684333/converting-xml-to-dictionary-using-elementtree
    # by K3---rnc
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k:v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[t.tag].update((k, v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
                d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d

def queryScihub(wkt = None, 
                 tile = None, 
                 start='2020-06-01', 
                 end=now, 
                 cloudCover= 34.5, 
                 producttype = 'S2MSI1C',   
                 orderby = 'beginposition',
                 sort = 'desc',
                 valPixThresh = 0,
                 searchStr = None
               ):
    loginScihub()
    # only sentinel 2 supported at the moment
    platformname = 'Sentinel-2'
    cloudCover = defineCloudCover(cloudCover)
    fname       = defineFname(tile)
    footprint   = defineFootprint(wkt)
    time        = defineTime(start, end)
    platformProducttype = definePlatformProducttype(platformname, producttype)
    tail = defineTail(orderby, sort)
    
    if footprint is None:
        components = [time, platformProducttype, cloudCover, fname]
    else:
        components = [footprint, time, platformProducttype, cloudCover, fname]
        
    componentStr = ' AND '.join(components) + tail
    url = base + componentStr
    r = requests.get(url, auth = auth)
    if r.ok: 
        feed = r.json()['feed']
        total = int(feed['opensearch:totalResults'])
        pages = int(np.ceil(total/100.))
        logger.info ('Total %d files in %d page(s)'%(total, pages))
        logger.info ('searching page 1 of %d'%pages)
        ret = parse_feed(feed, valPixThresh)
        if total >= 100:
            par = partial(searchPage, pages=pages, url=url, auth=auth, valPixThresh=valPixThresh)         
            rets = list(map(par, range(1, pages)))
            for iret in rets:
                ret +=iret
    else:
        ret = []
        logger.info(r.content)  
    if searchStr is not None:
        logger.info ('Filter results based on search string: %s'% searchStr)
        temp = []
        for i in ret:
            urls = searchBand(i, searchStr, auth)
            i[4] = urls
            temp.append(i)
        ret = temp
    temp = []
    for i in ret:
        feed = namedtuple('feed', 'tile date footprint cloudCover downloadUrls quickView validPixelPercentage')
        temp.append(feed(*i))
    return temp

def downS2FileGoogle(feeds, s2FileDir = './'):
    urlFnames = []
    for i in feeds:
        urls = i.downloadUrls
        tile = i.tile
        mgrsTile = tile.split('_')[-2][1:]
        tileDir = '/'.join([mgrsTile[:2], mgrsTile[2], mgrsTile[3:]])
        if (len(urls) == 1) & (tile not in urls[0]):
            fname = tile + '.SAFE'
            fname = os.path.join(s2FileDir, fname)
            createParentDir(fname)
            urlFnames += [[urls[0],  fname]]
            url = 'gs://gcp-public-data-sentinel-2/tiles/' + tileDir + '/' + tile + '.SAFE'
            subprocess.call(['gsutil', '-m', 'cp', '-n', '-r', url, s2FileDir])
        else:
            # download individual files 
            for url in urls:
                fname = url.replace("')/Nodes('", '/').replace("')/$value", "").split('/')[7:]
                urlFname = '/'.join(fname)
                sysFname = os.path.join(*fname)
                sysFname = os.path.join(s2FileDir, sysFname)
                createParentDir(sysFname)
                urlFnames += [[url, sysFname]]
                url = 'gs://gcp-public-data-sentinel-2/tiles/' + tileDir + '/' + urlFname
                subprocess.call(['gsutil', 'cp', '-n', url, sysFname])
    
    return urlFnames

def existGoogle(feeds):
    '''
    Use google return code to test file existence,
    but this may be slow...
    So probally just use try, except...
    '''
    urlFnames = []
    for i in feeds:
        urls = i.downloadUrls
        tile = i.tile
        mgrsTile = tile.split('_')[-2][1:]
        tileDir = '/'.join([mgrsTile[:2], mgrsTile[2], mgrsTile[3:]])
        if (len(urls) == 1) & (tile not in urls[0]):
            # This is a whole Sentinel 2
            fname = tile + '.SAFE'
            fname = os.path.join(s2FileDir, fname)
            urlFnames += [[urls[0],  fname]]
            createParentDir(fname)
            url = 'gs://gcp-public-data-sentinel-2/tiles/' + tileDir + '/' + tile + '.SAFE'
            child = subprocess.Popen(['gsutil', '-q', 'stat', url + '/*'], stdout=subprocess.PIPE) 
            streamdata = child.communicate()[0] 
            returnCode = child.returncode
        else:
            # Download individual files 
            for url in urls:
                fname = url.replace("')/Nodes('", '/').replace("')/$value", "").split('/')[7:]
                urlFname = '/'.join(fname)
                sysFname = os.path.join(*fname)
                sysFname = os.path.join(s2FileDir, sysFname)
                createParentDir(sysFname)
                urlFnames += [[url, sysFname]]
                url = 'gs://gcp-public-data-sentinel-2/tiles/' + tileDir + '/' + urlFname
                child = subprocess.Popen(['gsutil', '-q', 'stat', url], stdout=subprocess.PIPE) 
                streamdata = child.communicate()[0] 
                returnCode = child.returncode
    
    return urlFnames

def downS2GoogleScihub(feeds, s2FileDir = './'):
    '''
    Getting S2 data from Google first,
    if not exist, then try Scihub source.
    '''
    urlFnames = []
    scihub = []
    for i in feeds:
        urls = i.downloadUrls
        tile = i.tile
        mgrsTile = tile.split('_')[-2][1:]
        tileDir = '/'.join([mgrsTile[:2], mgrsTile[2], mgrsTile[3:]])
        if (len(urls) == 1) & (tile not in urls[0]):
            # This is a whole Sentinel 2
            fname = tile + '.SAFE'
            fname = os.path.join(s2FileDir, fname)
            createParentDir(fname)
            urlFnames += [[urls[0],  fname]]
            logger.info('Getting data from Google source')
            url = 'gs://gcp-public-data-sentinel-2/tiles/' + tileDir + '/' + tile + '.SAFE'
            returnCode = subprocess.call(['gsutil', '-m', 'cp', '-n', '-r', url, s2FileDir])
            if returnCode == 1:
                logger.info('Failed to get data from Google and adding file to Scihub downloading queue.')
                scihub.append(i)
        else:
            # Download individual files
            lostUrls = []
            for url in urls:
                fname = url.replace("')/Nodes('", '/').replace("')/$value", "").split('/')[7:]
                urlFname = '/'.join(fname)
                sysFname = os.path.join(*fname)
                sysFname = os.path.join(s2FileDir, sysFname)
                createParentDir(sysFname)
                urlFnames += [[url, sysFname]]
                url = 'gs://gcp-public-data-sentinel-2/tiles/' + tileDir + '/' + urlFname
                returnCode = subprocess.call(['gsutil', 'cp', '-n', url, sysFname])
                if returnCode == 1:
                    logger.info('Failed to get data from Google and adding file to Scihub downloading queue.')
                    lostUrls.append(url)
            if len(lostUrls) > 0:
                i = i._replace(downloadUrls=lostUrls)
                scihub.append(i)
    if len(scihub) > 0:
        logger.info('Start to get lost data from Scihub.')
        downS2FileScihub(scihub, s2FileDir = s2FileDir)
        
    return urlFnames


def downS2FileScihub(feeds, s2FileDir = './'):
    urlFnames = []
    for i in feeds:
        urls = i.downloadUrls
        tile = i.tile
        # download the whole tile
        if (len(urls) == 1) & (tile not in urls[0]):
            fname = tile + '.zip'
            fname = os.path.join(s2FileDir, fname)
            urlFnames += [[urls[0],  fname]]
        else:
            # download individual files 
            for url in urls:
                fname = url.replace("')/Nodes('", '/').replace("')/$value", "").split('/')[7:]
                fname = os.path.join(*fname)
                urlFnames += [[url, fname]]
    # only two parallel processes allowed by scihub
    p = Pool(2)
    ret = p.map(downloader, urlFnames)
    p.close()
    p.join()
    return urlFnames

def createParentDir(fname):
    parentDir = os.path.dirname(fname)
    try: 
        os.makedirs(parentDir)
    except OSError:
        if not os.path.isdir(parentDir):
            raise

def downloader(urlFname):
    url, fname = urlFname
    createParentDir(fname)
    logger.info('Downloading %s'%fname.split('/')[-1])
    if os.path.exists(fname) | os.path.exists(fname.replace('.zip', '.SAFE')):
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
        if fname.endswith('.zip'):
            logger.info('Extracting %s'%fname.split('/')[-1])
            with zipfile.ZipFile(fname, 'r') as zip_ref:
                zip_ref.extractall(os.path.dirname(fname))
            logger.info('Remove %s'%fname.split('/')[-1])
            os.remove(fname)
            
def getS2Files(aoi = None, 
               tiles = None, 
               latlon = None, 
               start = '2015-12-01', 
               end = now, 
               cloudCover = 100, 
               producttype = 'S2MSI1C',   
               orderby = 'beginposition',
               sort = 'desc',
               valPixThresh = 0,
               searchStr = None,
               s2FileDir = './'
               ):
    wkts = []
    rets = []
    if aoi is not None:
        wkts += parse_aoi(aoi = aoi)
    if latlon is not None:
        wkts += parse_aoi(latlon = latlon)
    for wkt in wkts:
        rets += queryScihub(wkt = wkt, 
                             tile = None, 
                             start=start, 
                             end=end, 
                             cloudCover= cloudCover, 
                             producttype = producttype,   
                             orderby = orderby,
                             sort = sort,
                             valPixThresh = valPixThresh,
                             searchStr = searchStr,
                            )

    if tiles is not None:
        tiles = np.atleast_1d(tiles).tolist()
        for tile in tiles:
            rets += queryScihub(wkt = None, 
                                 tile = tile, 
                                 start=start, 
                                 end=end, 
                                 cloudCover= cloudCover, 
                                 producttype = producttype,   
                                 orderby = orderby,
                                 sort = sort,
                                 valPixThresh = valPixThresh,
                                 searchStr = searchStr,
                                )
    downS2GoogleScihub(rets, s2FileDir)  
    
    s2TileDirs = []
    for i in rets:
        s2TileDir = os.path.join(s2FileDir, i.tile + '.SAFE')
        s2TileDirs.append(os.path.realpath(s2TileDir))
    
    return s2TileDirs

def test1():
    print('Test tile, cloud, search string...\n')
    ret = queryScihub(tile='50SMJ', cloudCover=1, searchStr = '*B01.jp2')
    urlFnames = downS2FileGoogle(ret)
    urlFnames = downS2FileScihub(ret)
    print('Done!\n')

def test2():
    ao = 'POLYGON((115.79984234354565 39.41267418434987,' + \
                  '115.81853363330639 39.41267418434987,' + \
                  '115.81853363330639 39.42542974293974,' + \
                  '115.79984234354565 39.42542974293974,' + \
                  '115.79984234354565 39.41267418434987))'
    print('Test aoi WKT string, cloud, searchStr...\n')
    ret = queryScihub(wkt = ao, cloudCover=1, searchStr = '*B01.jp2')
    urlFnames = downS2FileGoogle(ret)
    urlFnames = downS2FileScihub(ret)
    print('Done!\n')
    
def test3():
    print('Test  aoi WKT string, time, searchStr...\n')
    ret = queryScihub(tile='50SMG',start='2020-06-01', end='2020-06-04', searchStr = '*B01.jp2')
    urlFnames = downS2FileScihub(ret)
    urlFnames = downS2FileGoogle(ret)
    print('Done!\n')

def test4():
    print('Test tile, time...\n')
    ret = queryScihub(tile='50SMG',start='2020-06-01', end='2020-06-04')
    urlFnames = downS2FileGoogle(ret)
    urlFnames = downS2FileScihub(ret)
    print('Done!\n')

def test5():
    print('Test tile, time, cloud cover...\n')
    ret = queryScihub(tile='50SMG',start='2020-06-01', cloudCover=20)
    urlFnames = downS2FileScihub(ret)
    urlFnames = downS2FileGoogle(ret)
    print('Done!\n')
    
def test6():
    print('Test multiple pages searching...\n')
    ret = queryScihub(tile='50SMG', start='2015-06-01')
    print('Done!\n')
    
def test7():
    print('Test download Google first then Scihub\n')
    ret = queryScihub(tile='50SMG',start='2020-06-01', cloudCover=20)
    urlFnames = downS2GoogleScihub(ret)
    print('Done!\n')
    
def test8():
    print('Test get S2 files\n')
    ao = 'POLYGON((115.79984234354565 39.41267418434987,' + \
              '115.81853363330639 39.41267418434987,' + \
              '115.81853363330639 39.42542974293974,' + \
              '115.79984234354565 39.42542974293974,' + \
              '115.79984234354565 39.41267418434987))'
    ret = getS2Files(tiles='50SMG', aoi = ao, start='2020-06-01', cloudCover=20)
    return ret
    print('Done!\n')
    
if __name__ == '__main__':
    test1()
    test2()
    test3()
    test4()
    test5()
    test6()
    test7()
    ret = test8()