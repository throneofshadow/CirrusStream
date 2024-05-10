import pandas as pd
import pdb
import os


class SystemMonitoring:
    def __init__(self, transmission_log_address='/home/ubuntu/data/streamlog.txt', time_out_address = '/home/ubuntu/data/systemd_log.txt'):
        self.transmission_log_address = transmission_log_address
        self.time_out_address = time_out_address
        try:
            self.transmission_log = pd.read_csv(self.transmission_log_address)
        except:
            os.system('touch ' + self.transmission_log_address)  # Create file.
        try:
            self.time_out_log = pd.read_csv(self.time_out_address)
        except:
            # Create new log file and read in data.
            os.system('touch ' + time_out_address)
        self.check_last_transmission()
        self.everything = True
        self.event_list = []
        self.sent_alert = False
        self.num_alert_loops = 0
        self.num_loops = 0
        self.last_touch = 0
        self.alert_count = 0
        first_time = self.check_last_transmission(last_time=False)
        while self.everything == True:
            if self.num_loops == 0:
                self.touch_time = self.check_last_transmission(last_time=first_time)
                self.last_touch = self.touch_time
                touch_time_s = 0  # just starting
                #print(self.touch_time, touch_time_s)
            else:
                #print(self.last_touch)
                self.touch_time = self.check_last_transmission(last_time=self.last_touch)
            if int(self.touch_time-self.last_touch) != 0 and self.num_loops > 0:
                touch_time_s = 1
                self.alert_count = 0  # reset counter if non-zero value
            else:
                self.alert_count += 1
                touch_time_s = 0
            if len(self.event_list) > 40:
                self.event_list = self.event_list[-39:-1]  # Keep only these entries
                self.event_list.append(touch_time_s)
                #print(touch_time_s)
            else:
                #print(touch_time_s)
                # Var: first_time_s, self.event_list, first_time
                self.event_list.append(touch_time_s)
            #pdb.set_trace()
            if len(self.event_list) < 30:
                #print('Still fetching data')
                alert_state='s'
            elif self.alert_count > 30 and self.num_loops > 5:
                self.do_notification_alert()
                self.num_alert_loops += 1
                self.alert_count += 1
                self.sent_alert = True
                alert_state='alert'
                #print('Alert')
            else:
#                pdb.set_trace()
                  # self.event_list, self.touch_time, touch_time_s
                #print('awake')
                alert_state = 'awake'
            self.last_touch = self.touch_time
            self.num_loops += 1
            f=open('/home/ubuntu/data/instance_log.txt', 'a')
            f.write(str(touch_time_s) + ',' + str(alert_state) + '\n')
            f.close()
            os.system('sleep 5')

    def refresh_log_files(self):
        self.transmission_log = pd.read_csv(self.transmission_log_address)
        self.time_out_log = pd.read_csv(self.time_out_address)

    def do_notification_alert(self):
        if self.sent_alert and self.num_alert_loops > 0:
            if self.num_alert_loops > 30:
                self.sent_alert=False
                self.num_alert_loops = 0
            else:
                self.num_alert_loops += 1
        else:
            os.system('touch /home/ubuntu/data/streaming_alert.txt')
            #os.system('aws s3 cp /home/ubuntu/data/streaming_alert.txt s3://streamingawsbucket/logging/streaming_alert.txt')
            os.system('python3 send_email.py')
            print('Sent alert')
            self.sent_alert = True
            self.num_alert_loops += 1

    def check_last_transmission(self,last_time = True, verbose=True):
        filename = "/home/ubuntu/data/streamlog.txt"
        statbuf = os.stat(filename)
        if last_time:
            tt = statbuf.st_mtime
            time = tt - last_time
            if verbose:
                print("Modification time: " + str(time))
            return tt
        else:
            #pdb.set_trace()
            s = statbuf.st_mtime
            if verbose:
                print('loading time')
            return s
        #transmission_check = self.transmission_log.loc[:, -1]

SystemMonitoring()
