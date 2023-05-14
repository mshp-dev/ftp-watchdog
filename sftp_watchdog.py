#!/usr/bin/env python3

from threading import Thread
from threading import Event
from datetime import datetime as dt
from time import sleep
from logging import handlers
import os, sys, logging, paramiko, json


with open('config.json') as config_file:
    configs = json.load(config_file)

log_path        = configs['LOG_PATH']
check_interval  = configs['CHECK_INTERVAL']
local_paths     = configs['LOCAL_PATHS']
operation_modes = configs['OPERATION_MODES']
hosts           = configs['HOSTS']

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)-8s] (%(module)s.%(funcName)s) %(message)s',
    datefmt='[%Y-%m-%d %H:%M:%S]',
    handlers=[
        handlers.RotatingFileHandler(
            filename=log_path,
            mode='a',
            maxBytes=10485760,
            backupCount=30,
            encoding='utf-8'
        )
    ]
)


def locals_to_sftps(stop_event, remove_source_file, sftp_sessions):
    while True:
        sleep(check_interval)
        if stop_event.is_set():
            logging.warning('Stoping module "LOCALS_TO_SFTPS" execution and exit the thread.')
            break
        files_in_paths = []
        for path in local_paths:
            files_in_paths.append({
                'path': path,
                'files': os.listdir(path)
            })
        for fip in files_in_paths:
            if len(fip['files']) > 0:
                logging.info(f'Found some new files in {fip["path"]}: {fip["files"]}')
                for file_ in fip["files"]:
                    for session in sftp_sessions:
                        try:
                            session['client'].put(localpath=os.path.join(fip['path'], file_), remotepath=file_)
                        except Exception as e:
                            logging.error(e)
                            logging.warning(f'Error occurred when trying to put {file_} into {session["name"]}')
                    if remove_source_file:
                        try:
                            os.remove(os.path.join(fip['path'], file_))
                            logging.info(f'"{file_}" moved to sftps and removed from {fip["path"]}.')
                        except Exception as e:
                            logging.error(e)
                            logging.warning(f'Error occurred when trying to delete/remove {file_}')
                    else:
                        logging.info(f'"{file_}" moved from local to sftps.')


def sftps_to_locals(stop_event, remove_source_file, sftp_sessions):
    while True:
        sleep(check_interval)
        if stop_event.is_set():
            logging.warning('Stoping module "SFTPS_TO_LOCALS" execution and exit the thread.')
            break
        for session in sftp_sessions:
            files_in_path = session['client'].listdir()
            if len(files_in_path) > 0:
                logging.info(f'Found some new files in {session["name"]}: {files_in_path}')
                for path in local_paths:
                    for fip in files_in_path:
                        try:
                            session['client'].get(remotepath=fip, localpath=os.path.join(path, fip))
                        except Exception as e:
                            logging.error(e)
                            logging.warning(f'Error occurred when trying to get {fip} from {session["name"]}')
                for fip in files_in_path:
                    if remove_source_file:
                        try:
                            session['client'].remove(fip)
                            logging.info(f'"{fip}" moved to locals and removed from {session["name"]}.')
                        except Exception as e:
                            logging.error(e)
                            logging.warning(f'Error occurred when trying to delete/remove {fip}')
                    else:
                        logging.info(f'"{fip}" moved from locals to {session["name"]}.')


def sftps_to_sftps(stop_event, remove_source_file, keep_copy, sftp_sessions):
    while True:
        sleep(check_interval)
        if stop_event.is_set():
            logging.warning('Stoping module "SFTPS_TO_SFTPS" execution and exit the thread.')
            break
        source_sessions = [session for session in sftp_sessions if session['role'] == 'SOURCE']
        destination_sessions = [session for session in sftp_sessions if session['role'] == 'DESTINATION']
        for source_sess in source_sessions:
            files_in_path = source_sess['client'].listdir()
            if len(files_in_path) > 0:
                logging.info(f'Found some new files in {source_sess["name"]}: {files_in_path}')
                for path in local_paths:
                    for fip in files_in_path:
                        try:
                            source_sess['client'].get(remotepath=fip, localpath=os.path.join(path, fip))
                        except Exception as e:
                            logging.error(e)
                            logging.warning(f'Error occurred when trying to get {fip} from {source_sess["name"]}')
                        for destination_sess in destination_sessions:
                            try:
                                destination_sess['client'].put(localpath=os.path.join(path, fip), remotepath=fip)
                            except Exception as e:
                                logging.error(e)
                                logging.warning(f'Error occurred when trying to put {fip} into {destination_sess["name"]}')
                        if not keep_copy:
                            try:
                                os.remove(os.path.join(path, fip))
                            except Exception as e:
                                logging.error(e)
                                logging.warning(f'Error occurred when trying to delete/remove {fip}')
                for fip in files_in_path:
                    if remove_source_file:
                        try:
                            source_sess['client'].remove(fip)
                            logging.info(f'"{fip}" moved to locals and removed from {source_sess["name"]}.')
                        except Exception as e:
                            logging.error(e)
                            logging.warning(f'Error occurred when trying to delete/remove {fip}')
                    else:
                        logging.info(f'"{fip}" moved from locals to {source_sess["name"]}.')


def stop_all_operations(sftp_sessions, operation_stop_event):
    try:
        operation_stop_event.set()
        for session in sftp_sessions:
            session['client'].close()
            session['tp'].close()
    except Exception as e:
        logging.info('First run initialization.')


def initialize_operation_mode(sftp_sessions):
    for opr in operation_modes:
        if list(opr.values())[0]:
            if list(opr.keys())[0] == 'LOCALS_TO_SFTPS':
                logging.info('Starting module in "LOCALS_TO_SFTPS" mode in a seprate thread.')
                locals_to_sftps_stop_event = Event()
                locals_to_sftps_thread = Thread(
                    target=locals_to_sftps,
                    args=(
                        locals_to_sftps_stop_event,
                        list(opr.values())[1], #REMOVE_SOURCE_FILE
                        sftp_sessions
                    )
                )
                locals_to_sftps_thread.start()
                return locals_to_sftps_stop_event
            elif list(opr.keys())[0] == 'SFTPS_TO_LOCALS':
                logging.info('Starting module in "SFTPS_TO_LOCALS" mode in a seprate thread.')
                sftps_to_locals_stop_event = Event()
                sftps_to_locals_thread = Thread(
                    target=sftps_to_locals,
                    args=(
                        sftps_to_locals_stop_event,
                        list(opr.values())[1], #REMOVE_SOURCE_FILE
                        sftp_sessions
                    )
                )
                sftps_to_locals_thread.start()
                return sftps_to_locals_stop_event
            elif list(opr.keys())[0] == 'SFTPS_TO_SFTPS':
                logging.info('Starting module in "SFTPS_TO_SFTPS" mode in a seprate thread.')
                sftps_to_sftps_stop_event = Event()
                sftps_to_sftps_thread = Thread(
                    target=sftps_to_sftps,
                    args=(
                        sftps_to_sftps_stop_event,
                        list(opr.values())[1], #REMOVE_SOURCE_FILE
                        list(opr.values())[2], #KEEP_COPY_IN_LOCAL
                        sftp_sessions
                    )
                )
                sftps_to_sftps_thread.start()
                return sftps_to_sftps_stop_event
    return None


def initialize_sftp_sessions():
    sftp_sessions = []
    i = 0
    try:
        for host in hosts:
            session = {}
            session['addr'] = host['ADDRESS']
            session['port'] = host['PORT']
            session['usr'] = host['USERNAME']
            session['pwd'] = host['PASSWORD']
            session['path'] = host['PATH']
            session['role'] = host['ROLE']
            session['name'] = f"{session['usr']}@{session['addr']}->{host['PATH']}"
            session['tp'] = paramiko.Transport((session['addr'], int(session['port'])))
            session['tp'].connect(username=session['usr'], password=session['pwd'])
            session['client'] = paramiko.SFTPClient.from_transport(session['tp'])
            session['client'].chdir(session['path'])
            sftp_sessions.append(session)
            logging.info(f'Starting new sftp session: {session["name"]}.')
            i += 1
    except Exception as e:
        logging.error(e)
        logging.warning('Exiting with error.')
        sys.exit(1)
    finally:
        return sftp_sessions


def main():
    sftp_sessions = []
    logging.info('Initializing sftp sessions...')
    sftp_sessions = initialize_sftp_sessions()
    logging.info('Initializing operation mode...')
    operation_stop_event = initialize_operation_mode(sftp_sessions)
    if operation_stop_event:
        try:
            while True:
                sleep(check_interval)
                for session in sftp_sessions:
                    if not session['tp'].is_active():
                        stop_all_operations(sftp_sessions, operation_stop_event)
                        sftp_sessions = []
                        logging.info('Closed all remaining sessions.')
                        logging.info('Initializing sftp sessions...')
                        sftp_sessions = initialize_sftp_sessions()
                        logging.info('Initializing operation mode...')
                        operation_stop_event = initialize_operation_mode(sftp_sessions)
        except KeyboardInterrupt:
            stop_all_operations(sftp_sessions, operation_stop_event)
            logging.info('Closed all sessions and exit.')
        except Exception as e:
            logging.error(e)
            logging.warning('Exiting with error.')
            sys.exit(1)
    else:
        stop_all_operations(sftp_sessions, operation_stop_event)
        logging.warning('No operation mode enabled.')
        logging.warning('Check config file.')
        sys.exit(0)


if __name__ == "__main__":
    main()