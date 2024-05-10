import glob
import os
import pandas as pd
from datetime import datetime
import pdb


def send_file_home_to_s3(data_address = '/home/ubuntu/data/' , s3_address = 's3://streamingawsbucket/data/', move = True):
    if move:
        os.system('aws s3 mv ' + data_address + ' ' +  s3_address)
    else:
        os.system('aws s3 cp ' + data_address + ' ' + s3_address)
print('Starting file_crawler')
print('Cleaning data files')

data_file_path = '/home/ubuntu/data/'
s3_bucket_address = 's3://streamingawsbucket/data/'
dateToday = datetime.today().strftime('%Y-%m-%d')
year_today = dateToday[0:4]
month_today = dateToday[5:7]
day_today = dateToday[8:10]
print(year_today,month_today,day_today)
for files in glob.glob(data_file_path + '*_log.csv'): # Search for files using wild-care ending in .
    try:
        name_seg = files.split('/')[-1].split('_')
        fname_client = name_seg[0]
        fname_year = name_seg[1]
        fname_month = name_seg[2]
        fname_day = name_seg[3]
        print(files)
        print(fname_client, fname_year, fname_month, fname_day)
        file_ok = True
    except:
        print(name_seg)
        print('seg on this file')
        file_ok = False
    if file_ok and int(fname_year) == int(year_today) and int(fname_month) == int(month_today) and int(day_today) - 2 < int(fname_day) < int(day_today) + 2:
        transfer_file = False
    else:
        transfer_file=True
    print('transfer is  ' + str(transfer_file))
    if file_ok:
        send_file_home_to_s3(files, s3_bucket_address + fname_client + '/' + fname_year + '/' + fname_month + '/' + fname_day + '/' + files.split('/')[-1], move=transfer_file)
    #pdb.set_trace()
