from multiprocessing import pool
from multiprocessing.pool import Pool
import os, datetime, string
import logging
import fcculsget
import postgresql
import multiprocessing as mp
from zipfile import ZipFile

def unzip_ulsdb(datadir):
    logging.info('Entering unzip_ulsdb ...')

    pathdownload = datadir + 'download/'
    os.makedirs(pathdownload, exist_ok=True)    
    
    datazip = os.listdir(pathdownload)
    logging.info('Path of Zip files is : ' + pathdownload)

    for zipfile in datazip:
        pathraw = datadir + 'raw/' + os.path.basename(zipfile).removesuffix('.zip')
        os.makedirs(pathraw, exist_ok=True)

        zipfile = pathdownload + zipfile
        logging.info('Zip file to be extracted is : ' + zipfile)

        with ZipFile(os.path.join(datadir + 'download/', zipfile) , 'r') as zipObj:
            # Extract all the contents of zip file import
            zipObj.extractall(pathraw)

    logging.info('Exiting unzip_ulsdb ...')

def get_creation_date(pathcount):
    logging.info('Entering get_creation_date ...')

    if os.path.exists(pathcount):
        logging.info('Counts file is : ' + pathcount)
    else:
        logging.info('Invalid Counts file is : ' + pathcount)
    
    countsfile = open(pathcount, 'rt')
    countsline = countsfile.readline()
    countsline = countsline.rstrip('\n')

    logging.info('Date Line from count file is ||' + countsline + '||')

    datecomponents = countsline.split()

    datecode = datecomponents[8]

    match datecomponents[4]:
        case 'Jan':
            datecode = datecode + '01'
            logging.info('Month is January')
        case 'Feb':
            datecode = datecode + '02'
            logging.info('Month is February')
        case 'Mar':
            datecode = datecode + '03'
            logging.info('Month is March')
        case 'Apr':
            datecode = datecode + '04'
            logging.info('Month is April')
        case 'May':
            datecode = datecode + '05'
            logging.info('Month is May')
        case 'Jun':
            datecode = datecode + '06'
            logging.info('Month is June')
        case 'Jul':
            datecode = datecode + '07'
            logging.info('Month is July')
        case 'Aug':
            datecode = datecode + '08'
            logging.info('Month is August')
        case 'Sep':
            datecode = datecode + '09'
            logging.info('Month is September')
        case 'Oct':
            datecode = datecode + '10'
            logging.info('Month is October')
        case 'Nov':
            datecode = datecode + '11'
            logging.info('Month is November')
        case 'Dec':
            datecode = datecode + '12'
            logging.info('Month is December')

    if (int(datecomponents[5]) < 10):
        datecode = datecode + '0' + datecomponents[5]
    else:
        datecode = datecode + datecomponents[5]

    logging.info('Date Code is : ' + datecode)

    logging.info('Exiting get_creation_date ...')

    return datecode

def build_file_list(datadir):
    logging.info('Entering build_file_list ...')

    pathraw = datadir + 'raw/'
    os.makedirs(pathraw, exist_ok=True)

    dataraw = os.listdir(pathraw)

    if 'l_amat' in dataraw:
        logging.info('License Type is AMAT')
        weeklydata = 'l_amat'
    elif 'l_gmrs' in dataraw:
        logging.info('License Type is GMRS')
        weeklydata = 'l_gmrs'
    else:
        logging.info('ERROR: License Type is invalid exiting ...')
        exit()

    weekly_datecode = get_creation_date(pathraw + weeklydata + '/counts')
    list_newdata = [ weeklydata ]

    for rawdir in dataraw:
        if rawdir == weeklydata:
            logging.info('Weekly data skipped: ' + rawdir)
            continue

        logging.info('Daily data found: ' + rawdir)
        if (int(get_creation_date(pathraw + rawdir + '/counts')) <= int(weekly_datecode)):
            logging.info('Daily data is older than weekly - skipped: ' + rawdir)
            continue

        list_newdata.append(rawdir)
        logging.info('Daily data is newer than weekly - added: ' + rawdir)
 
    logging.info('Exiting build_file_list ...')
    return list_newdata

def combine_data(datadir, list_fileupdate, newfile):
    logging.info('Entering combine_files ...')

    pathraw = datadir + 'raw/'
    os.makedirs(pathraw, exist_ok=True)

    pathimport = datadir + 'import/'
    os.makedirs(pathimport, exist_ok=True)

    importfile = pathimport + newfile
    logging.info('Combining data files to build import file: ' + importfile)
    

    with open(importfile, 'w') as outfile:
        for dirpath in list_fileupdate:
            updatefile = pathraw + dirpath + '/' + newfile
            if not os.path.isfile(updatefile):
                logging.info('File does not exist: ' + updatefile)
                continue
            logging.info('File to merge updates: ' + updatefile)
            with open(updatefile) as infile:
                for line in infile:
                    outfile.write(line)

    if os.path.getsize(importfile) == 0:
        os.remove(importfile)

def combine_files(datadir, list_fileupdate):
    logging.info('Entering combine_files ...')

    pathraw = datadir + 'raw/'
    os.makedirs(pathraw, exist_ok=True)

    pathimport = datadir + 'import/'
    os.makedirs(pathimport, exist_ok=True)

    pool = []
    datatype_list = [ 'AM.dat', 'CO.dat', 'EN.dat', 'HD.dat', 'HS.dat', 'LA.dat', 'SC.dat', 'SF.dat']
    for newfile in datatype_list:
        p = mp.Process(target=combine_data, args=(datadir, list_fileupdate, newfile, ))
        p.start()
        pool.append(p)

    for p in pool:
        p.join()

    logging.info('Exiting combine_files ...')

def load_data(datadir, licensetype, options='none=none'):
    logging.info('Entering load_data ...')

    options_list = options.split()
    match options_list[0].split('=')[0]:
        case nodownload:
            if string.capwords(options_list[0].split('=')[1]) == 'Yes':
                ulsdownload = False
            else:
                ulsdownload = True

    loadstart = datetime.datetime.now()
    logging.info('Initiating data load of ' + licensetype + ' FCC ULD DB: ' + str(loadstart))

    match licensetype:
        case 'amat':
            logging.info('License Type is AMAT')
            if ulsdownload:
                p_weekly_amat = mp.Process(target=fcculsget.get_amat_weekly, args=(datadir, ))
                p_weekly_amat.start()
                logging.info('Process started for Weekly AMAT')

                p_daily_amat = mp.Process(target=fcculsget.get_amat_daily_all, args=(datadir, ))
                p_daily_amat.start()
                logging.info('Process started for Daily AMAT')

                p_weekly_amat.join()
                p_daily_amat.join()
                logging.info('Processes complete for downlaod of AMAT')
        case 'gmrs':
            logging.info('License Type is GMRS')
            if ulsdownload:
                p_weekly_gmrs = mp.Process(target=fcculsget.get_gmrs_weekly, args=(datadir, ))
                p_daily_gmrs = mp.Process(target=fcculsget.get_gmrs_daily_all, args=(datadir, ))
                logging.info('Processes assigned for GMRS')

                p_weekly_gmrs.start()
                p_daily_gmrs.start()
                logging.info('Processes started for download of GMRS')

                p_weekly_gmrs.join()
                p_daily_gmrs.join()
                logging.info('Processes started for download of GMRS')
        case _:
            logging.info('ERROR: License Type is invalid exiting ...')
            exit()
        
    unzip_ulsdb(datadir)
    list_updatefiles = build_file_list(datadir)

    combine_files(datadir, list_updatefiles)

    postgresql.import_db_data(datadir, licensetype)

    loadend = datetime.datetime.now()
    logging.info('Completed data load of ' + licensetype + ' FCC ULD DB: ' + str(loadend))
    logging.info('Elapsed Time of data load of ' + licensetype + ' FCC ULD DB: ' + str(loadend - loadstart))
    
    logging.info('Exiting load_data ...')
