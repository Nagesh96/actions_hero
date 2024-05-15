try:
    logging.info('Security Detail Came to try block')
    for filename in who.keys():
        f.retrbinary('RETR {}'.format(filename), open(who[filename], 'wb').write)
    logging.info('Security Detail line -138 file going to before quit')
    f.quit()
    logging.info('Security Detail file going to after quit')
    if f.close:
        logging.info('Security Detail if file closed')
    else:
        logging.info('Security Detail else file is NOT closed')
    logging.info('Security Detail file completed last line')

    logging.info('Security Detail Came to try block_step_1')
    files_path = os.path.join(os.getcwd(), sunday_date + '*Sec_Cond*.xls')
    logging.info('Security Detail -get_data method line 151 files_path is_step_2: ' + str(files_path))
    files = sorted(glob.iglob(files_path), key=os.path.getctime, reverse=True)
    logging.info('Security Detail -get_data method line-154 files is_step_3: ' + str(files))
except Exception as e:
    logging.info('Security Detail Came to except block')
    logging.info('Security Detail exception is: ' + str(e))
    logging.exception('Exception on downloading Security Detail files')
    self.batch_log_error(str(e))
    logging.exception('Security Detail Came to except block going to RETURN something')
    return
