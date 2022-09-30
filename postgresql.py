from asyncio.windows_events import NULL
import psycopg2, datetime, sys, os
from traceback import print_exc, print_stack, print_last
import logging
import multiprocessing as mp

amat_connection = psycopg2.connect(
    host="localhost",
    port="30432",
    database="amat",
    user="fcculs",
    password="%My-S3cr3t%",
)
amat_connection.autocommit = True

gmrs_connection = psycopg2.connect(
    host="localhost",
    port="30432",
    database="gmrs",
    user="fcculs",
    password="%My-S3cr3t%",
)
gmrs_connection.autocommit = True

def formatrec_cnt(rec_cnt):
    numberstring=""
    if(rec_cnt>=1000000000 or rec_cnt == 0):
        numberstring = numberstring + "{:03d}".format(rec_cnt//1000000000) + '.'
        rec_cnt = rec_cnt % 1000000000
    if(rec_cnt>=1000000 or rec_cnt == 0):
        numberstring = numberstring + "{:03d}".format(rec_cnt//1000000) + '.'
        rec_cnt = rec_cnt % 1000000
    if(rec_cnt>=1000 or rec_cnt == 0):
        numberstring = numberstring + "{:03d}".format(rec_cnt//1000) + '.'
        rec_cnt = rec_cnt % 1000
    numberstring = numberstring + "{:03d}".format(rec_cnt) + ''
    return numberstring

def import_am_data(datadir, licensetype):
    logging.info('Entering import_am_data ...')
    pathimport = datadir + 'import/'

    if licensetype == 'amat':
        logging.info('License type is set to: ' + licensetype)
        AM_cursor = amat_connection.cursor()
    elif licensetype == 'gmrs':
        logging.info('License type is set to: ' + licensetype)
        logging.info('AM.dat is not valid for ' + licensetype)
    else:
        logging.info('Invalid license type: ' + licensetype)
    
    logging.info('Importing data from AM.dat: ' + licensetype)

    rec_cnt = 0
    if licensetype == 'amat': 
        with open(pathimport + 'AM.dat') as infile:

            am_sql = """INSERT INTO am
                (
                    record_type, unique_system_identifier, uls_file_num, ebf_number, callsign,
                    operator_class, group_code, region_code, trustee_callsign, trustee_indicator,
                    physician_certification, ve_signature, systematic_callsign_change, vanity_callsign_change, vanity_relationship,
                    previous_callsign, previous_operator_class, trustee_name
                ) 
                VALUES(%s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s)
                ON CONFLICT (record_type, unique_system_identifier)
                DO UPDATE SET
                (
                    record_type, unique_system_identifier, uls_file_num, ebf_number, callsign,
                    operator_class, group_code, region_code, trustee_callsign, trustee_indicator,
                    physician_certification, ve_signature, systematic_callsign_change, vanity_callsign_change, vanity_relationship,
                    previous_callsign, previous_operator_class, trustee_name
                ) 
                = (%s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s);
                """

            for line in infile:
                line = line.rstrip()
                rec_cnt = rec_cnt + 1
                linecomponents = line.split('|')
                if rec_cnt % 10000 == 0:
                    logging.info('AM.dat Import Progress: ' + licensetype + ' ... ' + formatrec_cnt(rec_cnt))

                try:
                    AM_cursor.execute(am_sql, 
                        ( 
                        linecomponents[0], int(linecomponents[1]), linecomponents[2], linecomponents[3], linecomponents[4], 
                        linecomponents[5], linecomponents[6], linecomponents[7], linecomponents[8], linecomponents[9], 
                        linecomponents[10], linecomponents[11], linecomponents[12], linecomponents[13], linecomponents[14], 
                        linecomponents[15], linecomponents[16], linecomponents[17],

                        linecomponents[0], int(linecomponents[1]), linecomponents[2], linecomponents[3], linecomponents[4], 
                        linecomponents[5], linecomponents[6], linecomponents[7], linecomponents[8], linecomponents[9], 
                        linecomponents[10], linecomponents[11], linecomponents[12], linecomponents[13], linecomponents[14], 
                        linecomponents[15], linecomponents[16], linecomponents[17]
                        ))
                except:
                    print('AM file Components are (' + str(rec_cnt) + '): ', '[%s]' % ', '.join(map(str, linecomponents)))
                    print_last()

            logging.info('AM.dat Import Complete: ' + licensetype + ' ... ' + formatrec_cnt(rec_cnt) + '  <<<<<<<<<<\n\n')

def import_en_data(datadir, licensetype):
    logging.info('Entering import_en_data ...')
    pathimport = datadir + 'import/'

    if licensetype == 'amat':
        logging.info('License type is set to: ' + licensetype)
        EN_cursor = amat_connection.cursor()
    elif licensetype == 'gmrs':
        logging.info('License type is set to: ' + licensetype)
        EN_cursor = gmrs_connection.cursor()
    else:
        logging.info('Invalid license type: ' + licensetype)
    
    logging.info('Importing data from EN.dat: ' + licensetype)

    rec_cnt = 0
    with open(pathimport + 'EN.dat') as infile:

        en_sql = """INSERT INTO public.en
        (
            record_type, unique_system_identifier, uls_file_number, ebf_number, call_sign, 
            entity_type, licensee_id, entity_name, first_name, mi, 
            last_name, suffix, phone, fax, email, 
            street_address, city, state, zip_code, po_box,
            attention_line, sgin, frn, applicant_type_code, applicant_type_other, 
            status_code, status_date, lic_category_code, linked_license_id, linked_callsign
        ) VALUES(%s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s)
            ON CONFLICT (record_type, unique_system_identifier)
            DO UPDATE SET
            (
                record_type, unique_system_identifier, uls_file_number, ebf_number, call_sign, 
                entity_type, licensee_id, entity_name, first_name, mi, 
                last_name, suffix, phone, fax, email, 
                street_address, city, state, zip_code, po_box,
                attention_line, sgin, frn, applicant_type_code, applicant_type_other, 
                status_code, status_date, lic_category_code, linked_license_id, linked_callsign
            ) 
            = (%s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s);
            """

        for line in infile:
            line = line.rstrip()
            rec_cnt = rec_cnt + 1
            linecomponents = line.split('|')
            if rec_cnt % 10000 == 0:
                logging.info('EN.dat Import Progress: ' + licensetype + ' ... ' + formatrec_cnt(rec_cnt))
            try:
                EN_cursor.execute(en_sql, 
                    ( 
                    linecomponents[0], int(linecomponents[1]), linecomponents[2], linecomponents[3], linecomponents[4], 
                    linecomponents[5], linecomponents[6], linecomponents[7], linecomponents[8], linecomponents[9], 
                    linecomponents[10], linecomponents[11], linecomponents[12], linecomponents[13], linecomponents[14], 
                    linecomponents[15], linecomponents[16], linecomponents[17], linecomponents[18], linecomponents[19],
                    linecomponents[20], linecomponents[21], linecomponents[22], linecomponents[23], linecomponents[24], 
                    linecomponents[25], linecomponents[26], linecomponents[27], linecomponents[28], linecomponents[29],


                    linecomponents[0], int(linecomponents[1]), linecomponents[2], linecomponents[3], linecomponents[4], 
                    linecomponents[5], linecomponents[6], linecomponents[7], linecomponents[8], linecomponents[9], 
                    linecomponents[10], linecomponents[11], linecomponents[12], linecomponents[13], linecomponents[14], 
                    linecomponents[15], linecomponents[16], linecomponents[17], linecomponents[18], linecomponents[19],
                    linecomponents[20], linecomponents[21], linecomponents[22], linecomponents[23], linecomponents[24], 
                    linecomponents[25], linecomponents[26], linecomponents[27], linecomponents[28], linecomponents[29],
                    ))
            except:
                print('EN file Components are (' + str(rec_cnt) + '): ', '[%s]' % ', '.join(map(str, linecomponents)))
                print_last()
           

        logging.info('EN.dat Import Complete: ' + licensetype + ' ... ' + formatrec_cnt(rec_cnt) + '  <<<<<<<<<<\n\n')

def import_hd_data(datadir, licensetype):
    logging.info('Entering import_hd_data ...')
    pathimport = datadir + 'import/'

    if licensetype == 'amat':
        logging.info('License type is set to: ' + licensetype)
        HD_cursor = amat_connection.cursor()
    elif licensetype == 'gmrs':
        logging.info('License type is set to: ' + licensetype)
        HD_cursor = gmrs_connection.cursor()
    else:
        logging.info('Invalid license type: ' + licensetype)
    
    logging.info('Importing data from HD.dat: ' + licensetype)

    rec_cnt = 0
    with open(pathimport + 'HD.dat') as infile:

        hd_sql = """INSERT INTO hd
        (
            record_type, unique_system_identifier, uls_file_number, ebf_number, call_sign, 
            license_status, radio_service_code, grant_date, expired_date, cancellation_date, 
            eligibility_rule_num, applicant_type_code_reserved, alien, alien_government, alien_corporation, 
            alien_officer, alien_control, revoked, convicted, adjudged, 
            involved_reserved, common_carrier, non_common_carrier, private_comm, fixed, 
            mobile, radiolocation, satellite, developmental_or_sta, interconnected_service, 
            certifier_first_name, certifier_mi, certifier_last_name, certifier_suffix, certifier_title, 
            gender, african_american, native_american, hawaiian, asian, 
            white, ethnicity, effective_date, last_action_date, auction_id, 
            reg_stat_broad_serv, band_manager, type_serv_broad_serv, alien_ruling, licensee_name_change, 
            whitespace_ind, additional_cert_choice, additional_cert_answer, discontinuation_ind, regulatory_compliance_ind, 
            eligibility_cert_900, transition_plan_cert_900, return_spectrum_cert_900, payment_cert_900
        ) 
        VALUES(%s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s)
        ON CONFLICT (record_type, unique_system_identifier)
        DO UPDATE SET
            (
            record_type, unique_system_identifier, uls_file_number, ebf_number, call_sign, 
            license_status, radio_service_code, grant_date, expired_date, cancellation_date, 
            eligibility_rule_num, applicant_type_code_reserved, alien, alien_government, alien_corporation, 
            alien_officer, alien_control, revoked, convicted, adjudged, 
            involved_reserved, common_carrier, non_common_carrier, private_comm, fixed, 
            mobile, radiolocation, satellite, developmental_or_sta, interconnected_service, 
            certifier_first_name, certifier_mi, certifier_last_name, certifier_suffix, certifier_title, 
            gender, african_american, native_american, hawaiian, asian, 
            white, ethnicity, effective_date, last_action_date, auction_id, 
            reg_stat_broad_serv, band_manager, type_serv_broad_serv, alien_ruling, licensee_name_change, 
            whitespace_ind, additional_cert_choice, additional_cert_answer, discontinuation_ind, regulatory_compliance_ind, 
            eligibility_cert_900, transition_plan_cert_900, return_spectrum_cert_900, payment_cert_900 
            ) 
            = (%s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s, %s,   %s, %s, %s, %s);
            """
        for line in infile:
            line = line.rstrip()
            rec_cnt = rec_cnt + 1
            linecomponents = line.split('|')
            if rec_cnt % 10000 == 0:
                logging.info('HD.dat Import Progress: ' + licensetype + ' ... ' + formatrec_cnt(rec_cnt))

            try:
                HD_cursor.execute(hd_sql, 
                    ( 
                    linecomponents[0], int(linecomponents[1]), linecomponents[2], linecomponents[3], linecomponents[4], 
                    linecomponents[5], linecomponents[6], linecomponents[7], linecomponents[8], linecomponents[9], 
                    linecomponents[10], linecomponents[11], linecomponents[12], linecomponents[13], linecomponents[14], 
                    linecomponents[15], linecomponents[16], linecomponents[17], linecomponents[18], linecomponents[19],
                    linecomponents[20], linecomponents[21], linecomponents[22], linecomponents[23], linecomponents[24], 
                    linecomponents[25], linecomponents[26], linecomponents[27], linecomponents[28], linecomponents[29],
                    linecomponents[30], linecomponents[31], linecomponents[32], linecomponents[33], linecomponents[34], 
                    linecomponents[35], linecomponents[36], linecomponents[37], linecomponents[38], linecomponents[39], 
                    linecomponents[40], linecomponents[41], linecomponents[42], linecomponents[43], linecomponents[44], 
                    linecomponents[45], linecomponents[46], linecomponents[47], linecomponents[48], linecomponents[49],
                    linecomponents[50], linecomponents[51], linecomponents[52], linecomponents[53], linecomponents[54], 
                    linecomponents[55], linecomponents[56], linecomponents[57], linecomponents[58], 


                    linecomponents[0], int(linecomponents[1]), linecomponents[2], linecomponents[3], linecomponents[4], 
                    linecomponents[5], linecomponents[6], linecomponents[7], linecomponents[8], linecomponents[9], 
                    linecomponents[10], linecomponents[11], linecomponents[12], linecomponents[13], linecomponents[14], 
                    linecomponents[15], linecomponents[16], linecomponents[17], linecomponents[18], linecomponents[19],
                    linecomponents[20], linecomponents[21], linecomponents[22], linecomponents[23], linecomponents[24], 
                    linecomponents[25], linecomponents[26], linecomponents[27], linecomponents[28], linecomponents[29],
                    linecomponents[30], linecomponents[31], linecomponents[32], linecomponents[33], linecomponents[34], 
                    linecomponents[35], linecomponents[36], linecomponents[37], linecomponents[38], linecomponents[39], 
                    linecomponents[40], linecomponents[41], linecomponents[42], linecomponents[43], linecomponents[44], 
                    linecomponents[45], linecomponents[46], linecomponents[47], linecomponents[48], linecomponents[49],
                    linecomponents[50], linecomponents[51], linecomponents[52], linecomponents[53], linecomponents[54], 
                    linecomponents[55], linecomponents[56], linecomponents[57], linecomponents[58], 
                   ))
            except:
                print('HD file Components are (' + str(rec_cnt) + '): ', '[%s]' % ', '.join(map(str, linecomponents)))
                print_last()

        logging.info('HD.dat Import Complete: ' + licensetype + ' ... ' + formatrec_cnt(rec_cnt) + '  <<<<<<<<<<\n\n')


def import_hs_data(datadir, licensetype):
    logging.info('Entering import_hs_data ...')
    pathimport = datadir + 'import/'

    if licensetype == 'amat':
        logging.info('License type is set to: ' + licensetype)
        HS_cursor = amat_connection.cursor()
    elif licensetype == 'gmrs':
        logging.info('License type is set to: ' + licensetype)
        HS_cursor = gmrs_connection.cursor()
    else:
        logging.info('Invalid license type: ' + licensetype)
    
    logging.info('Importing data from HS.dat: ' + licensetype)

    rec_cnt = 0
    with open(pathimport + 'HS.dat') as infile:

        hs_sql = """INSERT INTO hs
        (
            record_type, unique_system_identifier, uls_file_number, callsign, log_date,
            code
         ) 
        VALUES(%s, %s, %s, %s, %s,   %s)
        ON CONFLICT (record_type, unique_system_identifier)
        DO UPDATE SET
            (
                record_type, unique_system_identifier, uls_file_number, callsign, log_date,
                code
            ) 
            =   (%s, %s, %s, %s, %s,   %s)
            """
        for line in infile:
            line = line.rstrip()
            rec_cnt = rec_cnt + 1
            linecomponents = line.split('|')
            if rec_cnt % 10000 == 0:
                logging.info('HS.dat Import Progress: ' + licensetype + ' ... ' + formatrec_cnt(rec_cnt))

            try:
                HS_cursor.execute(hs_sql, 
                    ( 
                    linecomponents[0], int(linecomponents[1]), linecomponents[2], linecomponents[3], linecomponents[4], 
                    linecomponents[5], 

                    linecomponents[0], int(linecomponents[1]), linecomponents[2], linecomponents[3], linecomponents[4], 
                    linecomponents[5], 
                   ))
            except:
                print('HS file Components are (' + str(rec_cnt) + '): ', '[%s]' % ', '.join(map(str, linecomponents)))
                print_last()

        logging.info('HS.dat Import Complete: ' + licensetype + ' ... ' + formatrec_cnt(rec_cnt) + '  <<<<<<<<<<\n\n')

def import_db_data(datadir, licensetype):
    logging.info('Entering import_db_data ...')

    p_am_data = mp.Process(target=import_am_data, args=(datadir, licensetype, ))
    p_am_data.start()
    logging.info('Process started for ' + str.upper(licensetype) + ' AM Data')

    p_en_data = mp.Process(target=import_en_data, args=(datadir, licensetype, ))
    p_en_data.start()
    logging.info('Process started for ' + str.upper(licensetype) + ' EN Data')

    p_hd_data = mp.Process(target=import_hd_data, args=(datadir, licensetype, ))
    p_hd_data.start()
    logging.info('Process started for ' + str.upper(licensetype) + ' HD Data')

    p_hs_data = mp.Process(target=import_hs_data, args=(datadir, licensetype, ))
    p_hs_data.start()
    logging.info('Process started for ' + str.upper(licensetype) + ' HS Data')
 
    p_am_data.join()
    p_en_data.join()
    p_hd_data.join()
    p_hs_data.join()
    logging.info('Processes complete for upload of ' + licensetype + 'into database')

    logging.info('Exiting import_db_data ...')
