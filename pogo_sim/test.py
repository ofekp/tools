import subprocess
import sys
import re
import urllib2
import threading
import time
from pexpect import pxssh
import base64
import re
from datetime import datetime, timedelta
from openpyxl import Workbook
from openpyxl import load_workbook
from multiprocessing import Pool, Process
from subprocess import PIPE, Popen
from threading import Thread
from Queue import Queue, Empty

username = "ofek"
hosts = ["visitor4.nads.bf1.yahoo.com"]
command = "tail -10 /home/y/logs/vespa/vespa.log"

pattern = re.compile(r'(\d*\.\d*)\s.*, .*beta=(.*), alpha=(.*)\}\].*')
timeout_sec = 20
time_between_checks = 30
#bucket_names = ["copyProdMain", "sonTurnOnPeriodicFileModelCache"]
bucket_names = ["son30MinTrainingIntervalFileCache", "son30MinTrainingInterval"]
search_string = "[SearchPctrModifiers{feature=section, label=4250754"

model_timestamp_pattern = re.compile(r'.*/modelFile_main_(\d*)_.*\s(\{.*\})')
# model_current_pattern = re.compile('.*\s(\{.*\})')
model_line_pattern = re.compile(r'.*\/modelFile_main_(\d*)_.*\s\{.*4250754\",\s*"alpha":\s*([0-9-.]*),\s*"beta":\s*([0-9-.]*)\}')
model_ts_map = {}

now = datetime.now() - timedelta(hours=3)  # UTC time
xlsx_file_ts = "%d%02d%02d%02d%02d" % (now.year, now.month, now.day, now.hour, now.minute)
xlsx_file = "test_" + xlsx_file_ts + ".csv"


def execute_command(host):
    print("Updating son models history from HDFS")

    try:
        hdfs_ssh = pxssh.pxssh()
        hdfs_ssh.login(host, "ofek")
        print("sending command [" + command + "]")
        hdfs_ssh.sendline(command)
        res = hdfs_ssh.expect(["Password for", pxssh.EOF, pxssh.TIMEOUT])
        if res == 0:
            with open('config', 'r') as config_file:
                base64pass = config_file.readline()
                password = base64.b64decode(base64pass)
                print("sending password")
                hdfs_ssh.sendline(password)
        hdfs_ssh.prompt(1000)
        print(hdfs_ssh.before)
        else:
            print("Error while sending password to host [" + host + "]")
    except Exception as e:
        print(e)

    print("DONE with command for host [" + host + "]")


def enqueue_log_output(ssh, queue):
    for line in iter(ssh.stdout.readline, b''):
        queue.put(line)

    for i, host in enumerate(hosts):
        execute_command(host);
        # t = threading.Thread(target=execute_command, args=(host,))
        #
        # # [creative 33139426555] [SearchPctrModifiers{feature=section, label=4250754, beta=0.04713, alpha=-3.498046}]
        # ssh = subprocess.Popen(["ssh", "%s" % host, command],
        #                     shell=False,
        #                     stdout=subprocess.PIPE,
        #                     stderr=subprocess.PIPE)
        #
        # t.start()
        # t.join()

