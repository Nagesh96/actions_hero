def get_data(self):
    logging.info('Starting Security Detail download process')
    final_df = pd.DataFrame()
    filelist = []

    try:
        # Segregate files based on specific labels in filenames
        DVlist = [file for file in files if 'DV' in file][:2]
        NTlist = [file for file in files if 'NT' in file][:2]
        SDlist = [file for file in files if 'SD' in file][:2]
        IRlist = [file for file in files if 'IR' in file][:2]
        EMlist = [file for file in files if 'EM' in file][:2]
        
        # Flatten the list of files
        filelist = DVlist + NTlist + SDlist + IRlist + EMlist
        logging.info(f'Files to process: {filelist}')

        for file in filelist:
            try:
                with open(file, 'r', errors='replace') as fo:
                    text = fo.readlines()
                    logging.info(f'Processing file: {file}')

                    # Data Extraction Logic
                    header = []
                    body = []

                    for line in text:
                        if line.startswith('<TR>'):
                            for x in text[text.index(line) + 1:text.index(line) + 18]:
                                if x.startswith('<TD class=x1101>'):
                                    header.append(x[16:-6])
                                else:
                                    body.append(x[16:-6])

                    composite_list = [body[x:x + 17] for x in range(0, len(body), 17)]
                    logging.info(f'Processed data for file: {file}')

                    # Convert extracted data into DataFrame and filter columns
                    df = pd.DataFrame(composite_list, columns=header)
                    df = df[['FND_CU', 'FND_SU', 'FND_SD', 'FND_TC', 'FND_U0']]
                    df['userbank'] = file[33:35]
                    df = df.drop_duplicates(keep='first')
                    
                    final_df = final_df.append(df)
                    logging.info(f'Updated final DataFrame: {final_df}')

            except UnicodeDecodeError as e:
                logging.error(f'UnicodeDecodeError while processing file: {file} - {e}')
                self.batch_log_error(str(e))
            except Exception as e:
                logging.error(f'Error while processing file: {file} - {e}')
                self.batch_log_error(str(e))
        
        logging.info('Completed processing files and merged into final DataFrame')

    except Exception as e:
        logging.error(f'Error during Security Detail download process: {e}')
        self.batch_log_error(str(e))

    # Drop duplicate entries based on columns of interest
    final_df = final_df.drop_duplicates(subset=['FND_CU', 'FND_SU', 'userbank'])
    logging.info(f'Final DataFrame after cleaning: {final_df}')
    
    return final_df
