import os
import requests
import numpy as np
from osgeo import ogr
from osgeo import osr
from functools import partial
from multiprocessing import Pool
from collections import defaultdict
from xml.etree import cElementTree as ET
from datetime import datetime, timedelta
from S2_TOA_TO_LAI.create_logger import create_logger

file_path = os.path.dirname(os.path.realpath(__file__))
logger = create_logger()

username, password = np.loadtxt(file_path + '/.scihub_auth', dtype=str)
auth = tuple([username, password])

source = osr.SpatialReference()
source.ImportFromEPSG(4326)
target = osr.SpatialReference()
target.ImportFromProj4('+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs')
transform = osr.CoordinateTransformation(source, target)
tile_area = 12064298699.657892

base = 'https://scihub.copernicus.eu/dhus/search?start=0&rows=100&q='
t_date = datetime(2015, 10, 1)

def find_all(location, start='2015-01-01', end=datetime.now().strftime('%Y-%m-%d'), cloud_cover='[0 TO 100]', product_type='S2MSI1C', search_by_tile=True, val_pix_thresh = 0):
    if isinstance(cloud_cover, (int, float)):
        cloud_cover = '[0 TO %s]'%cloud_cover
    elif isinstance(cloud_cover, str):
        cloud_cover = cloud_cover
    else:
        raise IOError('Cloud cover can only be number or string, e.g. "[0 TO 10.1]"')
    ret = []
    temp1 = None
    if datetime.strptime(end, '%Y-%m-%d') < t_date:
        search_by_tile = False
    elif datetime.strptime(start, '%Y-%m-%d') < t_date:
        logger.info ('Searching data after %s'%(t_date.strftime('%Y-%m-%d')))
        temp1 = '( footprint:" Intersects(%s)" ) AND ( beginPosition:[%sT00:00:00.000Z TO %sT23:59:59.999Z] '+\
               'AND endPosition:[%sT00:00:00.000Z TO %sT23:59:59.999Z] ) AND (platformname:Sentinel-2 AND producttype:%s '+\
               'AND cloudcoverpercentage:%s)&orderby=beginposition desc& &format=json'
    else:
        pass   
    if isinstance(search_by_tile, str):
        temp = '( beginPosition:[%sT00:00:00.000Z TO %sT23:59:59.999Z] '+\
               'AND endPosition:[%sT00:00:00.000Z TO %sT23:59:59.999Z] ) AND (platformname:Sentinel-2 AND producttype:%s '+\
               'AND cloudcoverpercentage:%s AND filename:S2?_MSIL1C_????????T??????_N*_R*_T'+search_by_tile +'*)&orderby=beginposition desc& &format=json'
                
        url = base + temp%(start, end, start, end, product_type, cloud_cover)
    elif search_by_tile:
        temp = '( footprint:" Intersects(%s)" ) AND ( beginPosition:[%sT00:00:00.000Z TO %sT23:59:59.999Z] '+\
               'AND endPosition:[%sT00:00:00.000Z TO %sT23:59:59.999Z] ) AND (platformname:Sentinel-2 AND producttype:%s '+\
               'AND cloudcoverpercentage:%s AND filename:S2?_MSIL1C_????????T??????_N*_R*_T*)&orderby=beginposition desc& &format=json'
        
        url = base + temp%(location, start, end, start, end, product_type, cloud_cover)
    else:
        temp = '( footprint:" Intersects(%s)" ) AND ( beginPosition:[%sT00:00:00.000Z TO %sT23:59:59.999Z] '+\
               'AND endPosition:[%sT00:00:00.000Z TO %sT23:59:59.999Z] ) AND (platformname:Sentinel-2 AND producttype:%s '+\
               'AND cloudcoverpercentage:%s)&orderby=beginposition desc& &format=json'
        url = base + temp%(location, start, end, start, end, product_type, cloud_cover)
    #print url
    r = requests.get(url, auth = auth)
    feed = r.json()['feed']
    total = int(feed['opensearch:totalResults'])
    pages = int(np.ceil(total/100.))
    logger.info ('Total %d files in %d pages'%(total, pages))
    logger.info ('searching page 1 of %d'%pages)
    if total > 0:
        if total == 1:
            feed['entry'] = [feed['entry']]
        for i in range(len(feed['entry'])):
            title = feed['entry'][i]['title']
            cloud = feed['entry'][i]['double']['content']
            durl  = feed['entry'][i]['link'][0]['href']#.replace('$value', '\$value')  
            qurl  = feed['entry'][i]['link'][2]['href']#.replace('$value', '\$value')  
            date  = feed['entry'][i]['date'][1]['content']
            for j in feed['entry'][i]['str']:
                if 'POLYGON ((' in j['content']:
                    foot = j['content']
                    if search_by_tile:
                        geom = ogr.CreateGeometryFromWkt(foot)
                        geom.Transform(transform)
                        val_pix = (geom.GetArea()/tile_area)*100.
                        if val_pix > val_pix_thresh:
                            ret.append([title, date, foot, cloud, durl, qurl, val_pix])
                    else:
                        val_pix = None
                        ret.append([title, date, foot, cloud, durl, qurl, val_pix])
        if total >= 100:
            par = partial(search_page, pages=pages, base=base, temp=temp, location=location, \
                          start=start, end=end, product_type=product_type, cloud_cover=cloud_cover,\
                         auth=auth, search_by_tile=search_by_tile, val_pix_thresh = val_pix_thresh)
            p = Pool(2)            
            
            rets = p.map(par, range(1, pages))
            for iret in rets:
                ret +=iret

    if temp1 is not None:
        logger.info ('Searching data before %s'%(t_date.strftime('%Y-%m-%d')))
        url = base + temp1%(location, start, t_date.strftime('%Y-%m-%d'), start, t_date.strftime('%Y-%m-%d'), product_type, cloud_cover) 
        #print url            
        r = requests.get(url, auth = auth)
        feed = r.json()['feed']           
        total = int(feed['opensearch:totalResults'])
        pages = int(np.ceil(total/100.))  
        logger.info ('Total %d files in %d pages'%(total, pages))               
        logger.info ('searching page 1 of %d'%pages)                               
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
            ret.append([title, date, foot, cloud, durl, qurl, None])                                
        if total >= 100:     
            par = partial(search_page, pages=pages, base=base, temp=temp1, location=location, \
                          start=start, end = t_date.strftime('%Y-%m-%d'), product_type=product_type, \
                          cloud_cover=cloud_cover, auth=auth, search_by_tile=search_by_tile, \
                          val_pix_thresh = val_pix_thresh)
            p = Pool(2)
            rets = p.map(par, range(1, pages))
            for iret in rets:
                ret +=iret
    return ret

def search_page(page, pages, base, temp, location,start, end, product_type, cloud_cover, auth, search_by_tile, val_pix_thresh):
    logger.info ('searching page %d of %d'%(page+1, pages))                            
    if isinstance(search_by_tile, str):
        url = base.replace('start=0&rows=100', 'start=%d&rows=100'%(page*100)) + \
                            temp%(start, end, start, end, product_type, cloud_cover)
    else:
        url = base.replace('start=0&rows=100', 'start=%d&rows=100'%(page*100)) + \
                            temp%(location, start, end, start, end, product_type, cloud_cover)
    r = requests.get(url, auth = auth)
    feed = r.json()['feed']                                                 
    ret = []
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
                if search_by_tile:
                    geom = ogr.CreateGeometryFromWkt(foot)
                    geom.Transform(transform)
                    val_pix = (geom.GetArea()/tile_area)*100.
                    if val_pix > val_pix_thresh:
                        ret.append([title, date, foot, cloud, durl, qurl, val_pix])
                else:
                    val_pix = None
                    ret.append([title, date, foot, cloud, durl, qurl, val_pix])
        #ret.append( [title, date, foot, cloud, durl, qurl, None])
    return ret

def query_sen2(location, start='2015-01-01', end=datetime.now().strftime('%Y-%m-%d'), cloud_cover='[0 TO 100]', product_type='S2MSI1C', search_by_tile=True, band = None, val_pix_thresh = -1000, one_by_one = True):
    ret = find_all(location, start, end, cloud_cover, product_type, search_by_tile, val_pix_thresh)
    if band is not None:
        par = partial(search_tile_and_band, search_by_tile = search_by_tile, band = band, auth = auth)
        p = Pool(2)
        ret = p.map(par, ret)
        ret = [i for i in ret if i is not None]
    else:
        if (type(search_by_tile) is str) & (datetime.strptime(start, '%Y-%m-%d')< t_date):
            par = partial(search_tile_and_band, search_by_tile = search_by_tile, band = None, auth = auth)
            p = Pool(2)                                         
            b_ret = [i for i in ret if datetime.strptime(':'.join(i[1].split(':')[:-1]), '%Y-%m-%dT%H:%M') < t_date]
            a_ret = [[i[0], i[4]] for i in ret if datetime.strptime(':'.join(i[1].split(':')[:-1]), '%Y-%m-%dT%H:%M') >= t_date]
            ret = p.map(par, b_ret)
            if one_by_one:
                ret = [i for i in ret if i is not None] + a_ret
            else:
                ret   = [[b_ret[i][0], b_ret[i][4]] for i in range(len(b_ret)) if ret[i] is not None] + a_ret
        else:
            ret = [[i[0], i[4]] for i in ret]
    return ret

def search_tile_and_band(re, search_by_tile, band, auth):
    urls = []
    url = "%s/Nodes('%s.SAFE')/"%(re[4].split('/$value')[0], re[0])
    r = requests.get(url + "Nodes('manifest.safe')/$value", auth = auth)
    e = ET.XML(r.content)                        
    d = etree_to_dict(e)                         
    dataObject = d['{urn:ccsds:schema:xfdu:1}XFDU']['dataObjectSection']['dataObject']
    fnames = [do['byteStream']['fileLocation']['href'].replace('./GRANULE', re[0] + '/GRANULE') for do in dataObject]
    for fname in fnames:                         
        if (str(search_by_tile) in fname) & (str(band) in fname):
            furl = fname.replace('/', "')/Nodes('").replace(".')/", url) + "')/$value"
            urls.append([fname, furl])
        elif (str(search_by_tile) in fname):                                          
            furl = fname.replace('/', "')/Nodes('").replace(".')/", url) + "')/$value"
            urls.append([fname, furl])
        elif str(band) in fname:
            furl = fname.replace('/', "')/Nodes('").replace(".')/", url) + "')/$value"
            urls.append([fname, furl])
    if len(urls)>0:
        return urls

def downdown(url_fname, auth):
    url, fname = url_fname
    r = requests.get(url, stream=True, auth=auth)
    with open(fname, 'wb') as f:
        for chunk in r.iter_content(chunk_size=10240): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)


def etree_to_dict(t):
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

if __name__ == '__main__':
    aoi = 'POLYGON((4.795757185813976 41.21247680330302,19.787525048248387 41.21247680330302,19.787525048248387 51.690725710472634,4.795757185813976 51.690725710472634,4.795757185813976 41.21247680330302))'
    ao = 'POLYGON((115.79984234354565 39.41267418434987,115.81853363330639 39.41267418434987,115.81853363330639 39.42542974293974,115.79984234354565 39.42542974293974,115.79984234354565 39.41267418434987))'
    #a = query_sen2(aoi, search_by_tile='33TVJ', end='2017-12-10', start='2016-01-01', val_pix_thresh=60, cloud_cover=20.1, band = 'B02')
    #b = query_sen2(aoi, search_by_tile='33TVJ', end='2017-12-10', start='2016-01-01', val_pix_thresh=60, cloud_cover=20.1, band = None)
    #c = query_sen2(ao, search_by_tile=False, end='2017-12-10', start='2016-01-01', val_pix_thresh=60, cloud_cover=20.1, band=None, one_by_one = True)

    tiles = ['50SMJ', '50SLJ', '50SLH', '50SMH', '50SLG', '50SMG']  
    cs = [] 
    for tile in tiles: 
        c = query_sen2(ao, search_by_tile=tile, start='2018-01-02', end='2019-01-15', val_pix_thresh=-10000000000000, cloud_cover=100, band=None, one_by_one=True) 
        cs+=c 




