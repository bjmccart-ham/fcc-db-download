import requests, os, sys, datetime
import logging

#Get the file from the FCC

def formatfilesize(filesize):
    numberstring=""
    if(filesize>1000000000):
        numberstring = numberstring + "{:03d}".format(filesize//1000000000) + '.'
        filesize = filesize % 1000000000
    if(filesize>1000000):
        numberstring = numberstring + "{:03d}".format(filesize//1000000) + '.'
        filesize = filesize % 1000000
    if(filesize>1000):
        numberstring = numberstring + "{:03d}".format(filesize//1000) + '.'
        filesize = filesize % 1000
    numberstring = numberstring + "{:03d}".format(filesize) + ''
    return numberstring

def getulsdb(fccurl, filelocal):
    getulsstart = datetime.datetime.now()
    logging.info('Initiating file download of FCC ULD DB: ' + str(getulsstart))

    logging.info('FCC URL is :' + fccurl + ':')
    logging.info('Local File is :' + filelocal + ':')

    # Download the file form the FCC
    response = requests.get(fccurl, stream = True)

    filesize = 0
    chunk_size = 1024
    chunk_count = 0
    logging.info('ULS DB update download in progress:' + filelocal)

    filedownload = open(filelocal, 'wb')
    for chunk in response.iter_content(chunk_size=chunk_size):
        # Periodic file writing based on chunk_size
        filedownload.write(chunk)

        filesize = filesize + chunk_size
        chunk_count = chunk_count + 1
        if(chunk_count % 10240 == 0):
            logging.info(filelocal + ' Progress: ' + formatfilesize(filesize))

    filedownload.close()
    logging.info(filelocal + ' Complete: ' + formatfilesize(filesize) + '\n\n')
    
    # Retrieve HTTP meta-data
    logging.info(str(response.status_code))
    logging.info(str(response.headers['content-type']))
    logging.info(str(response.encoding))
    getulsend = datetime.datetime.now()
    logging.info('Completed file download of FCC ULD DB (' + filelocal + '): ' + str(getulsend))
    logging.info('Elapsed Time is(' + filelocal + '): ' + str(getulsend - getulsstart))

    return filedownload

def get_amat_daily_all(datadir):
    daily_url = 'https://data.fcc.gov/download/pub/uls/daily/'
    daily_file = ['l_am_sun.zip', 'l_am_mon.zip', 'l_am_tue.zip', 'l_am_wed.zip', 'l_am_thu.zip', 'l_am_fri.zip', 'l_am_sat.zip' ]
    downloaddir = os.path.join(datadir, 'download')

    logging.info('datadir : ' + datadir)
    logging.info('daily_url : ' + daily_url)
    logging.info('daily_file : ' + str(daily_file))
    logging.info('downloaddir : ' + downloaddir)

    os.makedirs(downloaddir, exist_ok=True)

    for idx in range(7):
        getulsdb(daily_url + daily_file[idx], os.path.join(downloaddir,daily_file[idx]))

    return daily_file

def get_amat_daily_single(datadir, day):
    daily_url = 'https://data.fcc.gov/download/pub/uls/daily/'
    daily_file = 'l_am_' + day + '.zip'
    downloaddir = os.path.join(datadir, 'download')
    os.makedirs(downloaddir, exist_ok=True)
    
    getulsdb(daily_url + daily_file, downloaddir.join(daily_file))

    return daily_file

def get_amat_weekly(datadir):
    weekly_url = 'https://data.fcc.gov/download/pub/uls/complete/'
    weekly_file = 'l_amat.zip'
    downloaddir = os.path.join(datadir, 'download')

    logging.info('datadir : ' + datadir)
    logging.info('weekly_url : ' + weekly_url)
    logging.info('weekly_file : ' + weekly_file)
    logging.info('downloaddir : ' + downloaddir)

    os.makedirs(downloaddir, exist_ok=True)

    getulsdb(weekly_url + weekly_file, os.path.join(downloaddir, weekly_file))
    
    return weekly_file

def get_gmrs_daily_all(datadir):
    daily_url = 'https://data.fcc.gov/download/pub/uls/daily/'
    daily_file = ['l_gm_sun.zip', 'l_gm_mon.zip', 'l_gm_tue.zip', 'l_gm_wed.zip', 'l_gm_thu.zip', 'l_gm_fri.zip', 'l_gm_sat.zip' ]
    downloaddir = os.path.join(datadir, 'download')

    logging.info('datadir : ' + datadir)
    logging.info('daily_url : ' + daily_url)
    logging.info('daily_file : ' + str(daily_file))
    logging.info('downloaddir : ' + downloaddir)

    os.makedirs(downloaddir, exist_ok=True)

    for idx in range(7):
        getulsdb(daily_url + daily_file[idx], os.path.join(downloaddir, daily_file[idx]))

    return daily_file

def get_gmrs_daily_single(datadir, day):
    daily_url = 'https://data.fcc.gov/download/pub/uls/daily/'
    daily_file = 'l_gm_' + day + '.zip'
    os.makedirs(datadir, exist_ok=True)
    
    getulsdb(daily_url + daily_file, os.path.join(datadir, daily_file))

    return daily_file

def get_gmrs_weekly(datadir):
    weekly_url = 'https://data.fcc.gov/download/pub/uls/complete/'
    weekly_file = 'l_gmrs.zip'
    downloaddir = os.path.join(datadir, 'download')

    logging.info('datadir : ' + datadir)
    logging.info('weekly_url : ' + weekly_url)
    logging.info('weekly_file : ' + weekly_file)
    logging.info('downloaddir : ' + downloaddir)

    os.makedirs(downloaddir, exist_ok=True)

    getulsdb(weekly_url + weekly_file, os.path.join(downloaddir, weekly_file))
    
    return weekly_file
