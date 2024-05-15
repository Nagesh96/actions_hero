# Conversion of dictionary
who = dict(zip(filelist, datelist))
logging.info('Dictionary created from filelist and datelist: {}'.format(who))

# Downloading files using retrbinary
try:
    logging.info('Security Detail Came to try block')
    for filename in who:
        file_path = who[filename]
        f.retrbinary('RETR {}'.format(filename), open(file_path, 'wb').write)
    logging.info('Security Detail line before quitting FTP')
    f.quit()
    logging.info('Security Detail FTP connection closed')
    if f.close:
        logging.info('Security Detail FTP connection is closed')
    else:
        logging.info('Security Detail FTP connection is NOT closed')
    logging.info('Security Detail file download completed')
    
    # Additional steps after downloading files
    files_path = os.path.join(os.getcwd(), sunday_date + '*Sec_Cond*.xls')
    logging.info('Security Detail files path: {}'.format(files_path))
    files = sorted(glob.iglob(files_path), key=os.path.getctime, reverse=True)
    logging.info('Security Detail sorted files: {}'.format(files)) 
except Exception as e:
    logging.info('Security Detail Came to except block')
    logging.info('Security Detail exception: {}'.format(e))
    logging.exception('Exception on downloading Security Detail files')
    self.batch_log_error(str(e))
    logging.exception('Security Detail Came to except block, returning')
    return
