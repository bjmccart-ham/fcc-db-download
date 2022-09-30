#from tkinter.messagebox import YESNOCANCEL
#import fcculsget
import fcculsload

#Testing

#dataamat = 'C:/fccdata/amat/'
#datagmrs = 'C:/fccdata/gmrs/'
datadirbase = 'C:/fccdata/'

fcculsload.process_ulsdb (datadirbase, options='none=none')

#fcculsload.load_data(dataamat, 'amat')
#fcculsload.load_data(datagmrs, 'gmrs')

#fcculsload.load_data(dataamat, 'amat', 'nodownload=Yes')
#fcculsload.load_data(datagmrs, 'gmrs', 'nodownload=Yes')

#fcculsload.load_amat(dataamat)
#fcculsload.load_gmrs(datagmrs)
#convert2csv(dataamat)

#fcculsget.get_amat_daily_all(dataamat)

#fcculsget.get_amat_daily_single(dataamat, 'sun')
#fcculsget.get_amat_daily_single(dataamat, 'mon')
#fcculsget.get_amat_daily_single(dataamat, 'tue')
#fcculsget.get_amat_daily_single(dataamat, 'wed')
#fcculsget.get_amat_daily_single(dataamat, 'thu')
#fcculsget.get_amat_daily_single(dataamat, 'fri')
#fcculsget.get_amat_daily_single(dataamat, 'sat')

#fcculsget.get_amat_weekly(dataamat)

#fcculsget.get_gm_daily_all(datagmrs)

#fcculsget.get_gm_daily_single(datagmrs, 'sun')
#fcculsget.get_gm_daily_single(datagmrs, 'mon')
#fcculsget.get_gm_daily_single(datagmrs, 'tue')
#fcculsget.get_gm_daily_single(datagmrs, 'wed')
#fcculsget.get_gm_daily_single(datagmrs, 'thu')
#fcculsget.get_gm_daily_single(datagmrs, 'fri')
#fcculsget.get_gm_daily_single(datagmrs, 'sat')

#fcculsget.get_gm_weekly(datagmrs)