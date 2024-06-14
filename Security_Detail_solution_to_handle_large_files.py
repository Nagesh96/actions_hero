import os
import logging
import pandas as pd
import datetime
from sqlalchemy import create_engine
import glob
import time
import ftplib
import sys

class SecurityDetail:

    def __init__(self, batch_id, params):
        self.batch_id = batch_id
        self.params = params
        logging.getLogger().setLevel(logging.INFO)
        self.error_status = False

    def batch_log_success(self):
        logging.info('Started Security Detail - batch_log_success method')
        engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, fast_executemany=True)
        batch_id = str(self.batch_id)
        exec_string = """       
        DECLARE @curDate datetime2(7)
        SET @curDate = CURRENT_TIMESTAMP
        UPDATE [daedbo].[dae_fabi_batch_log] set s_batch_end_date = @curDate where i_batch_id = ?
        UPDATE [daedbo].[dae_fabi_batch_log] set t_batch_status = 'SUCCESS' where i_batch_id = ?
        """
        try:
            connection = engine.raw_connection()
            cursor = connection.cursor()
            cursor.execute(exec_string, (batch_id, batch_id))
            cursor.commit()
            connection.close()
        except Exception as e:
            logging.error(f'Exception on batch log success update: {e}')
            self.batch_log_error(str(e))

    def batch_log_error(self, error_summary):
        logging.info('Started Security Detail - batch_log_error method')
        engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % self.params, fast_executemany=True)
        batch_id = str(self.batch_id)
        self.error_status = True
        exec_string = """
        DECLARE @curDate datetime2(7)
        SET @curDate CURRENT_TIMESTAMP
        UPDATE [daedbo].[dae_fabi_batch_log] set s_batch_end_date = @curDate where i_batch_id = ?
        UPDATE [daedbo].[dae_fabi_batch_log] set t_batch_status = 'ERROR' where i_batch_id = ?
        UPDATE [daedbo].[dae_fabi_batch_log] set t_batch_error_msg = ? where i_batch_id = ?
        """
        try:
            connection = engine.raw_connection()
            cursor = connection.cursor()
            cursor.execute(exec_string, (batch_id, batch_id, error_summary, batch_id))
            cursor.commit()
            connection.close()
        except Exception as e:
            logging.error(f'Exception on batch log error update: {e}')
            self.batch_log_error(str(e))

    def get_data(self):
        logging.info('Starting Security Detail download process')
        final_df = pd.DataFrame()
        filelist = []

        try:
            ftp_host = os.environ.get('FTP_SERVER')
            ftp_user = os.environ.get('FTP_USERNAME')
            ftp_pass = os.environ.get('FTP_PASSWORD')

            with ftplib.FTP(ftp_host) as f:
                f.login(user=ftp_user, passwd=ftp_pass)
                f.cwd('/ftp-fund/Capacity Model/')

                data = []
                f.dir(data.append)
                datelist = []
                today = datetime.date.today()
                weekday_1 = today.weekday() + 1 % 7
                sunday_date = today - datetime.timedelta(weekday_1)
                sunday_date = sunday_date.strftime("%Y%m%d")

                data = [file for file in data if sunday_date in file and 'Sec_Cond' in file]

                for line in data:
                    col = line.split()
                    date_str = ' '.join(line.split()[:2])
                    date = time.strptime(date_str, '%m-%d-%y %H:%M%p')
                    datelist.append(date)
                    filelist.append(col[3])

                combo = zip(datelist, filelist)
                who = dict(combo)

                for key in who:
                    with open(who[key], 'wb') as f_out:
                        f.retrbinary('RETR %s' % who[key], f_out.write)

            files_path = os.path.join(os.getcwd(), sunday_date + '*Sec_Cond*.xls')
            files = sorted(glob.iglob(files_path), key=os.path.getctime, reverse=True)

            DVlist = [file for file in files if 'DV' in file][:2]
            NTlist = [file for file in files if 'NT' in file][:2]
            SDlist = [file for file in files if 'SD' in file][:2]
            IRlist = [file for file in files if 'IR' in file][:2]
            EMlist = [file for file in files if 'EM' in file][:2]

            filelist = DVlist + NTlist + SDlist + IRlist + EMlist

            for file in filelist:
                with open(file, 'r', errors='replace') as fo:
                    text = fo.readlines()
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
                    df = pd.DataFrame(composite_list, columns=header)
                    df = df[['FND_CU', 'FND_SU', 'FND_SD', 'FND_TC', 'FND_U0']]
                    df['userbank'] = file[33:35]
                    df = df.drop_duplicates(keep='first')

                    final_df = final_df.append(df)

            final_df = final_df.drop_duplicates(subset=['FND_CU', 'FND_SU', 'userbank'])

            logging.info('Completed processing files and merged into final DataFrame')

        except UnicodeDecodeError as e:
            logging.error(f'UnicodeDecodeError while processing file: {file} - {e}')
            self.batch_log_error(str(e))

        except Exception as e:
            logging.error(f'Error during Security Detail download process: {e}')
            self.batch_log_error(str(e))

        logging.info(f'Final DataFrame after cleaning: {final_df}')
        return final_df

    def run_job(self):
        logging.info('Starting Security Detail job')
        final_df = self.get_data()

        if final_df.empty:
            self.batch_log_error('No data fetched or processing failed')
        else:
            self.batch_log_success()
        logging.info('Completed Security Detail job')


if __name__ == "__main__":
    batch_id = 123  # Example batch ID
    params = 'your_connection_string'  # Replace with the actual connection string

    security_detail = SecurityDetail(batch_id, params)
    security_detail.run_job()
