import subprocess
import sys
import re
import urllib2
import threading
import time
from pexpect import pxssh
import os


username = "ofek"
host = "visitor4.nads.bf1.yahoo.com"
# Ports are handled in ~/.ssh/config since we use OpenSSH
command = "tail -f /home/y/logs/vespa/vespa.log"
pattern = '(\d*\.\d*)\s.*, .*beta=(.*), alpha=(.*)\}\].*'
timeout_sec = 20
time_between_checks = 60
bucket_names = ["sonTurnOnPeriodicFileModelCache", "prodMain"]
search_string = ".*\[creative \d*\] \[SearchPctrModifiers\{feature=section, label=4250754.*"  # 31475648317
prompt = r'(\d{10}\.\d{3})\s' + host.replace(".", "\\.") + '\s\d+\/\d+\scontainer\d+\s[a-zA-Z.]+\sinfo\s\[creative \d+\]\s\[SearchPctrModifiers\{feature=section, label=4250754, beta=(-?\d+\.\d+), alpha=(-?\d+\.\d+)\}\]'  # 31475648317


def make_call(bucket):
    print("executing call")
    with open("profile", "r") as profile_file:
        data = profile_file.readline()
        cont_len = len(data)
        req = urllib2.Request("http://" + host + ":4080/search/?debug=true&queryProfile=curveball&bucketId=" + bucket + "&useOrganicFederations=true&useDPA=false&enableRtb=false&presentation.format=xml&timeout=10000", data, {'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': cont_len})
        f = urllib2.urlopen(req)
        response = f.read()
        f.close()
    print("Thread done")


try:
    ssh = pxssh.pxssh()
    # ssh.logfile = sys.stdout
    ssh.login(host, "ofek")
    ssh.sendline(command)
    ssh.prompt(10)
    print(ssh.before)
except pxssh.ExceptionPxssh as e:
    print("pxssh failed to start")
    print(e)


while True:
    for bucket_name in bucket_names:
        t = threading.Thread(target=make_call, args=(bucket_name,))

        try:
            t.start()
            res = ssh.expect([bucket_name, pxssh.EOF, pxssh.TIMEOUT], timeout=timeout_sec)
            if res == 0:
                res = ssh.expect([prompt, pxssh.EOF, pxssh.TIMEOUT], timeout=timeout_sec)
                if res == 0:
                    # print("yay!")
                    # print(ssh.before)
                    m = ssh.match
                    timestamp = m.group(1)
                    beta = m.group(2)
                    alpha = m.group(3)
                    print("[" + timestamp + "] bucket [" + bucket_name + "] alpha [" + alpha + "] beta [" + beta + "]")
                elif res == 2:
                    print("bucket [" + bucket_name + "] TIMEOUT")
                else:
                    print("bucket [" + bucket_name + "] Not sure what happened...")
            elif res == 2:
                print("bucket [" + bucket_name + "] TIMEOUT while waiting for bucket name to appear")
            else:
                print("bucket [" + bucket_name + "] Not sure what happened while waiting for bucket name to appear...")
        except pxssh.TIMEOUT as e:
            print("Timeout occurred")

        t.join()  # wait for the thread to finish

        # ssh.try_read_prompt(4)
        # os.read(ssh.child_fd, 255)
        # time.sleep(10)
        try:
            clear_time = time.time() + 10
            while True:
                os.read(ssh.child_fd, 255)
                if time.time() > clear_time:
                    break
            os.read(ssh.child_fd, 255)
        except e:
            print(e)
        #ssh.expect([pxssh.TIMEOUT, pxssh.EOF], timeout=)

    print("")
    # os.read(ssh.child_fd, 255)
    # time.sleep(time_between_checks)
    # ssh.try_read_prompt(time_between_checks / 3)
    #ssh.read_nonblocking(timeout=time_between_checks)
    time.sleep(time_between_checks)
    try:
        clear_time = time.time() + 10
        while True:
            os.read(ssh.child_fd, 255)
            if time.time() > clear_time:
                break
        os.read(ssh.child_fd, 255)
    except e:
        print(e)
    # ssh.expect([pxssh.TIMEOUT, pxssh.EOF], timeout=time_between_checks)

