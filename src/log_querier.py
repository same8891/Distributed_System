from distutils.dir_util import copy_tree
import os
import re
import socket
import threading
import time
import globals
import utils
import configs
import logging
import membership

def init():
    global logger, output_locker
    logger = logging.getLogger('log_querier')
    output_locker = threading.Lock()

def log_querier_server():
    """
    server program that listens on (HOST, PORT)
    takes in the request, executes grep(), and return grep()'s result to requester
    return: None
    """
    logger.info("log querier started")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((utils.get_host_name(), configs.log_querier_port))
    s.listen()

    while True:
        if globals.host_status != utils.Status.ACTIVE:
            continue
        conn, addr = s.accept()
        logger.info("connection from: " + str(addr))
        while True:
            request = conn.recv(4096).decode("utf-8")
            if request:
                if request == 'ACK':
                    logger.info("connection from: " + str(addr) + "ended!")
                    break
                logger.info("client request: " + str(request))
                result = grep(request)
                logger.info("result is: %s" %result)
                conn.send(result.encode("utf-8"))

def query(request):
    """
    send the query request and get data in 10 virtual machines
    return: None
    """
    global total_lines, output, logger, output_locker
    output = []
    total_lines = 0
    time_start = time.time()
    threads = []
    #==================================
    # query machines in membership list
    # for host_name, host_ip, id in membership.get_membership_message():
    #     t = threading.Thread(target = query_sender, args = (request, host_ip, configs.log_querier_port))
    #     threads.append(t)
    #     t.start()

    # query machines in membership list
    for host_ip in configs.machine_list:
        t = threading.Thread(target = query_sender, args = (request, host_ip, configs.log_querier_port))
        threads.append(t)
        t.start()
    #==================================

    for t in threads:
        t.join()

    time_end = time.time()
    time_cost = time_end-time_start
    output_locker.acquire()
    output.append(("total time cost of grep query is %f seconds\n" %time_cost))
    output.append(("Total lines are %d" % total_lines))
    output_locker.release()
    return output

def query_sender(command, host, port):
    """
    get the data from server and return the required information
    param command: the gerp command
    param host: host name
    param port: port number
    return: None
    """
    global total_lines, output, logger, output_locker
    try:
        time_start = time.time()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host,port))
            s.sendall(command.encode('utf-8'))
            data = b''
            deadline = time.time() + 5
            while True:
                bytes_received = s.recv(4096)
                data += bytes_received
                if bytes_received:
                    s.send(b'ACK')
                    break
                if time.time() > deadline:
                    logger.error("connection timeout exceeded 5 seconds")
                    return                
        time_end = time.time()
        time_cost = time_end - time_start

        received_data = data.decode('utf-8').strip("\n")
        filename, line_cnt = received_data.split(":")
        total_lines += int(line_cnt)
    except:
        return 
    output_locker.acquire()
    output.append(("[RESULT]"+("host is %s, " % host)))
    output.append(("{filename} has {line_count} matched lines, ".format(filename = filename, line_count = line_cnt)))
    output.append(("time cost is %f seconds\n" % time_cost))
    output_locker.release()

def grep(request):
    """
    executes `grep -c pattern`/ `grep -Ec pattern` on all *.log files in the same directory
    param request: the grep request from user
    return: line count message or error message
    """
    global logger
    args = request.split()
    if len(args) != 3 or args[0] != 'grep' or (args[1] != '-c' and args[1] != '-Ec'):
        return "Wrong format! Correct format: `grep -c pattern` or `grep -Ec pattern`"
    res = ''
    pattern = args[2]
    if pattern[0] == '\"' and pattern[-1] == '\"':
        pattern = pattern[1:-1]
    logger.info("updated pattern is: %s" %pattern)
    # put all files under current directory in a list
    files = os.listdir(configs.log_path)
    # filter to all *.log files
    log_files = [f for f in files if f.endswith('.log')]
    print(log_files)
    # iterate all log files
    for file in log_files:
        with open(file, encoding="utf-8") as f:
            c_cnt = ec_cnt = 0  # c_cnt is for exact pattern matching / ec_cnt is for regex pattern search
            for line in f:
                if line.count(pattern) > 0:
                    c_cnt += 1
                if len(re.findall(pattern, line)) > 0:
                    ec_cnt += 1
            if args[1] == '-c':
                res += "{file}:{line_count}\n".format(file=file, line_count=c_cnt)
            elif args[1] == '-Ec':
                res += "{file}:{line_count}\n".format(file=file, line_count=ec_cnt)
            else:
                return "Only grep -c / -Ec options are supported!"
    return res
