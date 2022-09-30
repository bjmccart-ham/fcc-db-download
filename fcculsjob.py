import multiprocessing as mp
import sys, logging
import fcculsload

def process_ulsdb (datadirbase, options='none=none'):
    logging.basicConfig(stream=sys.stdout, encoding='utf-8', level=logging.DEBUG)

    logging.info('Entering process_ulsdb ...')

    logging.info('__name__ is set to |' + __name__ + '|')
    if __name__ == "__main__":
        logging.info('Entering process_ulsdb (Main) ...')

        p_amat = mp.Process(target=fcculsload.load_data, args=(datadirbase + 'amat/', 'amat', options))
        p_amat.start()
    
        p_gmrs = mp.Process(target=fcculsload.load_data, args=(datadirbase + 'gmrs/', 'gmrs', options))
        p_gmrs.start()

        p_amat.join()
        p_gmrs.join()
    logging.info('Exiting process_ulsdb ...')

#process_ulsdb  ('C:/fccdata/')
process_ulsdb ('C:/fccdata/', options='nodownload=Yes')
