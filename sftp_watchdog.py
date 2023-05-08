#!/usr/bin/env python3

from datetime import datetime as dt
from time import sleep
from logging import handlers
import os, sys, logging, paramiko, json


with open('config.json') as config_file:
    configs = json.load(config_file)

setad_host     = configs['SETAD_HOST']
setad_port     = configs['SETAD_PORT']
setad_username = configs['SETAD_USERNAME']
setad_password = configs['SETAD_PASSWORD']
setad_path     = configs['SETAD_PATH']

sita2_host     = configs['SITA2_HOST']
sita2_port     = configs['SITA2_PORT']
sita2_username = configs['SITA2_USERNAME']
sita2_password = configs['SITA2_PASSWORD']
sita2_path     = configs['SITA2_PATH']

check_interval = configs["CHECK_INTERVAL"]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)-8s] (%(module)s.%(funcName)s) %(message)s',
    datefmt='[%Y-%m-%d %H:%M:%S]',
    handlers=[
        handlers.RotatingFileHandler(
            filename='sftp_watchdog.log',
            mode='a',
            maxBytes=10485760,
            backupCount=30,
            encoding='utf-8'
        )
    ]
)


def main():
    logging.info('Starting new sftp session as setad_client.')
    setad_tp = paramiko.Transport((setad_host, int(setad_port)))
    setad_tp.connect(username=setad_username, password=setad_password)
    setad_client = paramiko.SFTPClient.from_transport(setad_tp)
    setad_client.chdir(setad_path)
    logging.info('Starting new sftp session as sita2_client.')
    sita2_tp = paramiko.Transport((sita2_host, int(sita2_port)))
    sita2_tp.connect(username=sita2_username, password=sita2_password)
    sita2_client = paramiko.SFTPClient.from_transport(sita2_tp)
    sita2_client.chdir(sita2_path)
    logging.info('Checking for new files...')
    try:
        while True:
            sleep(check_interval)
            files_in_setad = setad_client.listdir()
            if len(files_in_setad) > 0:
                logging.info(f'Found some new files in setad: {files_in_setad}')
                for file_in_setad in files_in_setad:
                    file_in_local = file_in_sita2 = file_in_setad
                    setad_client.get(remotepath=file_in_setad, localpath=f'./{file_in_local}')
                    sita2_client.put(localpath=file_in_local, remotepath=file_in_sita2)
                    setad_client.remove(file_in_setad)
                    logging.info(f'"{file_in_setad}" moved from setad to sita2.')
                    os.system(f'del {file_in_local}') # delete file in windows (del)
                    # os.system(f'rm {file_in_local}') # delete file in linux (rm) 
    except KeyboardInterrupt:
        setad_client.close()
        sita2_client.close()
        logging.info('Close all sessions and exit.')
    except Exception as e:
        logging.error(e)
        logging.warning('Exiting with error.')
        sys.exit(1)


if __name__ == "__main__":
    main()