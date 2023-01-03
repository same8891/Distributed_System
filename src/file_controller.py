import logging
from symbol import pass_stmt
import time
import utils
import socket
import time
import json
import os
import struct
import threading
import collections
import math
import statistics
import traceback
import pickle
import gc
import numpy as np
import paramiko
import scp

import failure_detector
import configs
import communicator
import globals
import membership



class Metadata:
    def __init__(self) -> None:
        # file system
        self.file_to_ips = {} # {file_string: [ip1, ip2, ip3, ip4]}
        self.file_version = {} # {file_string: version_num}
        self.ip_to_files = {} # {ip: files_list}
        # ML job scheduling, job has unique id across model.
        self.model_to_job_queue = {} # {model: deque[job1, job2, job3]}
        self.job_info = {} # {job: [status, model, input_dataset_file_name, type]}
        # handle by job_scheduler
        self.job_to_resources = {} # {job: [ip1, ip2, ip3])}
        self.resource_to_queued_query_cnt = {} # {machine_ip: queued_query_cnt}
        # only for inference
        self.job_to_query_outputs = {} # {job: deque[output1, output2, output3]} 
        self.job_to_query_inputs = {} # {job: deque[cnt1, cnt2, cnt2]} 
        self.job_max_query_cnt = {} # {job: cnt}
        self.job_to_output_opend_file = {}# {job: opend_file}
        self.job_to_time = {} # {job: [start_time, acc_time]}
        
        self.job_to_query_replys = {} # {job: [query1, query2, query3]} query1 = (batch_num, correct_num, processing_time) or None
        self.job_dashboard = {} # {job: [accuracy, query_rate, query_processed_cnt, total_correct_num, total_data_num]}
        self.job_stat = {} # {job: (average, standard deviation, median, 90th percentile, 95th percentile, 99th percentile) of query processed time).
        self.working_file = {} # {job: file_path}
        self.batch_size = 50
        self.job_phase = utils.Job_Phase.NONE

def phase_end_clear_in_metadata():
    global metadata
    for model in metadata.model_to_job_queue.keys():
        metadata.model_to_job_queue[model] = []
    metadata.job_to_query_outputs.clear()
    metadata.job_to_resources.clear()
    metadata.resource_to_queued_query_cnt.clear()

    

def init():
    global master_ip, master_thread, t_master_should_stop, logger, metadata_lock, master_lock
    master_ip = ""
    master_thread = None
    t_master_should_stop = True
    logger = logging.getLogger('file_controller')
    # lock!!!!
    metadata_lock = threading.Lock()
    master_lock = threading.Lock()

def auto_store_master():
    #global t_master_should_stop
    #while not t_master_should_stop and globals.host_status == utils.Status.ACTIVE:
    #    time.sleep(configs.auto_store_master)
    #    store_master()
    pass

def recover_master():
    global metadata_lock, metadata
    metadata_lock.acquire()
    # restore metadata restore other working files.
    if os.path.exists(globals.pwd + "/sdfs_dir/metadata.pickle"):
        metadata= pickle.load(globals.pwd + "/sdfs_dir/metadata.pickle")
    # update self.job_to_time
    for job_id in metadata.job_to_time.keys():
        if job_id not in metadata.job_to_time:
            metadata.job_to_time[job_id] = [time.time(), 0]
        metadata.job_to_time[job_id][0] = time.time()
    metadata_lock.release()

def store_master():
    global metadata_lock, metadata
    print("Auto master saving...\n")
    metadata_lock.acquire()
    for path in metadata.working_file.values():
        communicator.request_delete(path.split('/')[-1])
    pickle.dump(metadata, globals.pwd + "/sdfs_dir/metadata.pickle")
    time.sleep(1)
    for host_name, ip, workspace_path, id in membership.get_membership_message():
        remote_wirte_file(ip, globals.pwd + "/sdfs_dir/metadata.pickle", workspace_path + "/sdfs_dir/metadata.pickle")
    for path in metadata.working_file.values():
        communicator.write_to_SDFS(path, path.split('/')[-1])
    print("Finish master saving!\n")
    metadata_lock.release()

def master():
    """"
    Serve as a master to deal with SDFS request
    Handle 6 types of message
    """
    global master_ip, master_thread, t_master_should_stop, logger, metadata, ack_list
    master_lock.acquire()
    logger.info("master started")
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((utils.get_host_ip(), configs.master_port))
    s.settimeout(configs.master_timeout)
    # metadata init
    metadata_lock.acquire()
    metadata = Metadata()
    metadata_lock.release()
    #recover_master()
    # run t_master_monitor
    t_master_monitor = threading.Thread(target=master_monitor)
    t_master_monitor.start()
    # run t_auto_store_master
    #t_auto_store_master = threading.Thread(target=auto_store_master)
    #t_auto_store_master.start()
    # leave or t_master_should_stop would cause master() finish
    while not t_master_should_stop and globals.host_status == utils.Status.ACTIVE:
        try:
            data, addr = s.recvfrom(4096)
            logger.info("connection from: " + str(addr) + " with data: " + data.decode())
            if data:
                # get message
                message =json.loads(data.decode())
                message_type = message[0]
                sender_ip = message[1]
                TTL = message[2]
                payload = message[3]
                # print(message_type,payload)
                # update timestamp
                failure_detector.set_timestamp(sender_ip, time.time())
                # skip message from non-member
                if not membership.is_member(sender_ip):
                    continue
                # check TTL
                if TTL == 0:
                    continue

                ########################
                # master message
                ########################
                if message_type == utils.MessageType.REQ_PUT:
                    # print("req put msg FROM requestor IN master func: ", message)
                    # print("From", (sender_ip))
                    # extract payload
                    sdfs_file_name = payload
                    metadata_lock.acquire()
                    if sdfs_file_name in metadata.file_to_ips:
                        locations_list = metadata.file_to_ips[sdfs_file_name]
                    else:
                        locations_list = file_locations_allocate(metadata)
                    metadata_lock.release()
                    ack_list = []
                    expect_ack_num = len(locations_list)
                    #t_master_ack_receiver = threading.Thread(target=master_ack_receiver, args=(expect_ack_num,))
                    #t_master_ack_receiver.start()

                    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    write_msg = [utils.MessageType.WRITE, utils.get_host_ip(), 1, sdfs_file_name]
                    for ip in locations_list:
                        # print("write msg To datanodes IN master func: ", write_msg)
                        # print("To", (ip, configs.listener_port))
                        ss.sendto(json.dumps(write_msg).encode(), (ip, configs.listener_port))
                    # check ack num (Consistent level W), in this place, we can check some version consistency
                    while len(ack_list) <= expect_ack_num:
                        if True or len(ack_list)>=configs.write_replica_num or len(ack_list) == expect_ack_num:
                            # manage metadata
                            metadata_lock.acquire()
                            if sdfs_file_name in metadata.file_to_ips:
                                metadata.file_version[sdfs_file_name] += 1
                            else:
                                metadata.file_to_ips[sdfs_file_name] = locations_list
                                metadata.file_version[sdfs_file_name] = 1
                                for ip in locations_list:
                                    if ip not in metadata.ip_to_files:
                                        metadata.ip_to_files[ip] = []
                                    metadata.ip_to_files[ip].append(sdfs_file_name)
                            metadata_lock.release()

                            # send ack to requester
                            metadata_lock.acquire()
                            ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, (locations_list, metadata.file_version[sdfs_file_name])]
                            metadata_lock.release()
                            # print("ack msg To requestor IN master func: ", ack_msg)
                            # print("To", (sender_ip, configs.request_ack_port))
                            ss.sendto(json.dumps(ack_msg).encode(), (sender_ip, configs.request_ack_port))
                            break # or wait until all ack receive, otherwise, if timeout, them do something.

                    ss.close()

                elif message_type == utils.MessageType.REQ_GET:
                    # extract payload
                    requestor_target_file_path = payload[0]
                    sdfs_file_name = payload[1]
                    version_num = payload[2]
                    
                    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    metadata_lock.acquire()
                    if sdfs_file_name in metadata.file_to_ips:
                        locations_list = metadata.file_to_ips[sdfs_file_name]
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, None]
                        ss.sendto(json.dumps(ack_msg).encode(), (sender_ip, configs.request_ack_port))
                        metadata_lock.release()
                    else:
                        error_ack_msg = [utils.MessageType.ERROR_ACK, utils.get_host_ip(), 1, None]
                        ss.sendto(json.dumps(error_ack_msg).encode(), (sender_ip, configs.request_ack_port))
                        metadata_lock.release()
                        continue

                    ack_list = []
                    expect_ack_num = len(locations_list)
                    # t_master_ack_receiver = threading.Thread(target=master_ack_receiver, args=(expect_ack_num,))
                    # t_master_ack_receiver.start()

                    read_msg = [utils.MessageType.READ, utils.get_host_ip(), 1, sdfs_file_name]
                    for ip in locations_list:
                        ss.sendto(json.dumps(read_msg).encode(), (ip, configs.listener_port))
                    
                    # check ack num (Consistent level R), in this place, we can check some version consistency
                    while len(ack_list) <= expect_ack_num:
                        if True or len(ack_list)>=configs.read_replica_num or len(ack_list) == expect_ack_num:
                            metadata_lock.acquire()
                            version_interval = [max(1, metadata.file_version[sdfs_file_name]-version_num+1), metadata.file_version[sdfs_file_name]]
                            get_data_msg = [utils.MessageType.GET_DATA, utils.get_host_ip(), 1, (requestor_target_file_path, sdfs_file_name ,sender_ip, version_interval)]
                            ss.sendto(json.dumps(get_data_msg).encode(), (locations_list[0], configs.listener_port))
                            metadata_lock.release()
                            break # or wait until all ack receive, otherwise, if timeout, them do something.

                    ss.close()

                elif message_type == utils.MessageType.REQ_DELETE:
                    # extract payload
                    sdfs_file_name = payload
                    # check file exists or not
                    metadata_lock.acquire()
                    if sdfs_file_name in metadata.file_to_ips:
                        locations_list = metadata.file_to_ips[sdfs_file_name]
                        metadata_lock.release()
                    else:
                        metadata_lock.release()
                        continue
                    # send Delete to locations_list
                    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    delete_msg = [utils.MessageType.DELETE, utils.get_host_ip(), 1, (sdfs_file_name, metadata.file_version[sdfs_file_name])]
                    for ip in locations_list:
                        ss.sendto(json.dumps(delete_msg).encode(), (ip, configs.listener_port))
                    ss.close()

                    # update metadat
                    metadata_lock.acquire()
                    for ip in metadata.file_to_ips[sdfs_file_name]:
                        metadata.ip_to_files[ip].remove(sdfs_file_name)
                    del metadata.file_to_ips[sdfs_file_name]
                    del metadata.file_version[sdfs_file_name]
                    metadata_lock.release()

                elif message_type == utils.MessageType.REQ_LIST:
                    # extract payload
                    sdfs_file_name = payload
                    
                    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    metadata_lock.acquire()
                    if sdfs_file_name in metadata.file_to_ips:
                        locations_list = metadata.file_to_ips[sdfs_file_name]
                        metadata_lock.release()
                    else:
                        error_ack_msg = [utils.MessageType.ERROR_ACK, utils.get_host_ip(), 1, None]
                        ss.sendto(json.dumps(error_ack_msg).encode(), (sender_ip, configs.request_ack_port))
                        metadata_lock.release()
                        continue
                    ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, locations_list]
                    # print("ls",ack_msg)
                    ss.sendto(json.dumps(ack_msg).encode(), (sender_ip, configs.request_ack_port))
                    ss.close()

                ########################
                # job scheduling message
                ########################
                elif message_type == utils.MessageType.CHANGE_PHASE:
                    metadata_lock.acquire()
                    new_phase = payload
                    if metadata.job_phase == utils.Job_Phase.NONE:
                        metadata.job_phase = new_phase
                        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        # send ACK with new_phase via UDP to Master's master_port
                        if new_phase == utils.Job_Phase.TRAINING:
                            ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Training phase successful start."]
                            # t_job_sheduler = threading.Thread(target=job_sheduler)
                            # t_job_sheduler.start()
                        elif new_phase == utils.Job_Phase.INFERENCE:
                            ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Inference phase successful start."]
                            t_job_sheduler = threading.Thread(target=job_sheduler)
                            t_job_sheduler.start()
                            t_query_reply_receiver = threading.Thread(target=query_reply_receiver)
                            t_query_reply_receiver.start()

                        elif new_phase == utils.Job_Phase.NONE:
                            ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "No started phase."]
                        ss.sendto(json.dumps(ack_msg).encode(), (sender_ip, configs.request_ack_port))
                        ss.close()

                    elif metadata.job_phase == utils.Job_Phase.TRAINING:
                        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        # send ACK with new_phase via UDP to Master's master_port
                        if new_phase == utils.Job_Phase.NONE:
                            metadata.job_phase = new_phase
                            phase_end_clear_in_metadata()
                            ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Training phase successfully end."]
                        else:
                            ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Training phase is running."]
                        ss.sendto(json.dumps(ack_msg).encode(), (sender_ip, configs.request_ack_port))
                        ss.close()    

                    elif metadata.job_phase == utils.Job_Phase.INFERENCE:
                        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        # send ACK with new_phase via UDP to Master's master_port
                        if new_phase == utils.Job_Phase.NONE:
                            metadata.job_phase = new_phase
                            phase_end_clear_in_metadata()
                            ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Inference phase successfully end."]
                        else:
                            ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Inference phase is running."]
                        ss.sendto(json.dumps(ack_msg).encode(), (sender_ip, configs.request_ack_port))
                        ss.close()   
                    # print("change phase finish")    
                    metadata_lock.release()                 
                    
                elif message_type == utils.MessageType.ADD_JOB:
                    job_id, model, dataset_file_name = payload
                    metadata_lock.acquire()
                    # check job_id
                    if job_id in metadata.job_info:
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Job id exists."]
                        communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                        metadata_lock.release()
                        continue
                    metadata_lock.release()
                    if metadata.job_phase == utils.Job_Phase.TRAINING:
                        metadata_lock.acquire()
                        if model not in metadata.model_to_job_queue:
                            metadata.model_to_job_queue[model] = collections.deque()
                        metadata.model_to_job_queue[model].append(job_id)
                        metadata.job_info[job_id] = [utils.Job_Status.QUEUE, model, dataset_file_name, utils.Job_Type.TRAINING]
                        metadata_lock.release()
                        for host_name, ip, workspace_path, id in membership.get_membership_message():
                            dispatcher_msg = [utils.MessageType.DISPATCH, utils.get_host_ip(), 1, (None, metadata.job_info[job_id], None, None, None)]
                            communicator.udp_send(dispatcher_msg, ip, configs.listener_port)

                    elif metadata.job_phase == utils.Job_Phase.INFERENCE:
                        metadata_lock.acquire()
                        if model not in metadata.model_to_job_queue:
                            ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "No model exists."]
                            communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                            metadata_lock.release()
                            continue
                        metadata.model_to_job_queue[model].append(job_id)
                        metadata.job_info[job_id] = [utils.Job_Status.QUEUE, model, dataset_file_name, utils.Job_Type.INFERENCE]
                        metadata_lock.release()
                    
                    ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Successfully add job."]
                    communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)

                elif message_type == utils.MessageType.REMOVE_JOB:
                    job_id = payload
                    metadata_lock.acquire()
                    # check job_id
                    if job_id not in metadata.job_info:
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Job id not exists."]
                        communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                        metadata_lock.release()
                        continue
                    elif (metadata.job_phase == utils.Job_Phase.TRAINING and metadata.job_info[3]!=utils.Job_Type.TRAINING):
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "This is not a Training job."]
                        communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                        metadata_lock.release()
                        continue
                    elif (metadata.job_phase == utils.Job_Phase.INFERENCE and metadata.job_info[3]!=utils.Job_Type.INFERENCE):
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "This is not an Inference job."]
                        communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                        metadata_lock.release()
                        continue
                    model = metadata.job_info[job_id][1]
                    if os.path.exists(configs.sdfs_path + "output_job" + job_id + "_" + model+ ".txt.working"):
                        os.remove(configs.sdfs_path + "output_job" + job_id + "_" + model+ ".txt.working")
                    if os.path.exists(configs.sdfs_path + "input_job" + job_id + "_" + model+ ".txt.working"):
                        os.remove(configs.sdfs_path + "intput_job" + job_id + "_" + model+ ".txt.working")
                    communicator.request_delete(configs.sdfs_path + "output_job" + job_id + "_" + model+ ".txt")
                    communicator.request_delete(configs.sdfs_path + "output_job" + job_id + "_" + model+ ".txt.working")
                    metadata.model_to_job_queue[model].remove(job_id)
                    del metadata.job_info[job_id]
                    metadata.job_to_resources.pop(job_id)
                    metadata.job_to_query_outputs.pop(job_id)
                    metadata.job_to_query_inputs.pop(job_id)
                    metadata.job_to_query_replys.pop(job_id)
                    metadata.job_dashboard.pop(job_id)
                    metadata.job_max_query_cnt.pop(job_id)
                    metadata.job_to_output_opend_file.pop(job_id)
                    metadata.working_file.pop(job_id)
                    metadata.job_to_time.pop(job_id)
                    metadata_lock.release()
                    ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Successfully remove job."]
                    communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)

                elif message_type == utils.MessageType.REQUEST_QUERY_DASHBOARD:
                    job_id = payload
                    metadata_lock.acquire()
                    if job_id not in metadata.job_info:
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Job id not exists."]
                        communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                        metadata_lock.release()
                        continue
                    if job_id in metadata.job_dashboard:
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, (metadata.job_info[job_id], metadata.job_dashboard[job_id][0], metadata.job_dashboard[job_id][1], metadata.job_dashboard[job_id][2], metadata.job_max_query_cnt[job_id])]
                    elif job_id in metadata.job_max_query_cnt:
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, (metadata.job_info[job_id], "?", "?", 0, metadata.job_max_query_cnt[job_id])]
                    else:
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, (metadata.job_info[job_id], "?", "?", 0, "?")]
                    communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                    
                    metadata_lock.release()

                elif message_type == utils.MessageType.REQUEST_QUERY_STAT:
                    # self.job_query_stat = { } # {job: (average, standard deviation, median, 90th percentile, 95th percentile, 99th percentile) of query processed time).
                    job_id = payload
                    metadata_lock.acquire()
                    if job_id not in metadata.job_info:
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Job id not exists."]
                        communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                        metadata_lock.release()
                        continue
                    
                    if job_id in metadata.job_stat:
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, (metadata.job_info[job_id], metadata.job_stat[job_id][0],metadata.job_stat[job_id][1],metadata.job_stat[job_id][2],metadata.job_stat[job_id][3],metadata.job_stat[job_id][4],metadata.job_stat[job_id][5])]
                    else:
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Job is not yet finished."]
                    communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                    
                    metadata_lock.release()

                elif message_type == utils.MessageType.SET_BATCH_SIZE:
                    # prohibit set batch_size when there is job processing.
                    batch_size = payload
                    metadata_lock.acquire()
                    metadata.batch_size = int(batch_size)
                    ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Successfully set new batch size."]
                    communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                    metadata_lock.release()

                elif message_type == utils.MessageType.REQUEST_JOB_RESULT:
                    job_id = payload
                    metadata_lock.acquire()
                    if job_id not in metadata.job_info:
                        ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, "Job id not exists."]
                        communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                        metadata_lock.release()
                        continue
                    
                    ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, metadata.job_info[job_id]]
                    communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                    
                    metadata_lock.release()

                elif message_type == utils.MessageType.REQUEST_RESOURCE_DASHBOARD:
                    metadata_lock.acquire()
                    ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, (metadata.job_info, metadata.job_to_resources)]
                    communicator.udp_send(ack_msg, sender_ip, configs.request_ack_port)
                    metadata_lock.release()


                else:
                    logger.error("Unknown message type to master")
        except socket.timeout:
            pass
    metadata_lock.acquire()
    metadata = None
    metadata_lock.release()
    master_lock.release()

def job_sheduler():
    global t_master_should_stop, metadata_lock, metadata
    while not t_master_should_stop and globals.host_status == utils.Status.ACTIVE:
        time.sleep(configs.job_scheduler_period)
        metadata_lock.acquire()
        if  metadata.job_phase == utils.Job_Phase.NONE:
            metadata_lock.release()
            break
        metadata_lock.release()
        if metadata.job_phase == utils.Job_Phase.INFERENCE:
            # schedule job
            processing_job = []
            metadata_lock.acquire()
            for model, job_queue in metadata.model_to_job_queue.items():
                if len(job_queue)==0:
                    continue
                if metadata.job_info[job_queue[0]][0] == utils.Job_Status.PROCESSING:
                    processing_job.append(job_queue[0])
                    continue
                if len(job_queue)==0 or metadata.job_info[job_queue[0]][0]  == utils.Job_Status.FINISH:
                    communicator.write_to_SDFS(globals.pwd+"/sdfs_dir/output_job" + job_queue[0] + "_" + model +".txt.working", "output_job" + job_queue[0] + "_" + model +".txt")
                    os.remove(configs.sdfs_path + "output_job" + job_queue[0] + "_" + model+ ".txt.working")
                    communicator.request_delete(configs.sdfs_path + "output_job" + job_queue[0] + "_" + model+ ".txt.working")
                    metadata.working_file.pop(job_queue[0])
                    job_queue.popleft()

                while len(job_queue)!=0 and metadata.job_info[job_queue[0]][0]  == utils.Job_Status.ERROR:
                    job_queue.popleft()
                f1 = open(configs.sdfs_path + "output_job" + job_queue[0] + "_" + model + ".txt.working", "w")
                f1.close()
                f2 = open(configs.sdfs_path + "output_job" + job_queue[0] + "_" + model + ".txt.working", "a")
                metadata.job_to_output_opend_file[job_queue[0]] = f2
                metadata.working_file[job_queue[0]] = globals.pwd+"/sdfs_dir/output_job" + job_queue[0] + "_" + model +".txt.working"
                metadata_lock.release()
                t_job_dispatcher = threading.Thread(target=job_dispatcher, args=(job_queue[0],))
                t_job_dispatcher.start()
                t_redispatcher = threading.Thread(target=redispatcher, args=(job_queue[0],))
                t_redispatcher.start()
                metadata_lock.acquire()
                metadata.job_info[job_queue[0]][0] = utils.Job_Status.PROCESSING
                metadata.job_to_time[job_queue[0]] = [time.time(), 0]
                processing_job.append(job_queue[0])
            metadata_lock.release()
            if len(processing_job)==0:
                continue
            # # load balance
            # metadata_lock.acquire()
            # resources = [ip for host_name, ip, workspace_path, id in membership.get_membership_message()]
            # for job_id in processing_job:
            #     if job_id not in metadata.job_to_resources:
            #         metadata.job_to_resources[job_id] = []
            #     else:
            #         for r in metadata.job_to_resources[job_id]:
            #             if r not in resources:
            #                 metadata.job_to_resources[job_id].remove(r)
            # free_resources = collections.deque()
            # for r in resources:
            #     free = True
            #     for job_id, resource_list in metadata.job_to_resources.items():
            #         if r in resource_list:
            #             free = False
            #             break
            #     if free:
            #         free_resources.append(r)
            # no_resource_jobs = [job_id for job_id in processing_job if len(metadata.job_to_resources[job_id])==0]
            # resource_holder =[job_id for job_id in processing_job if len(metadata.job_to_resources[job_id])!=0]
            # metadata_lock.release()
            # while len(free_resources)!=0:
            #     if len(no_resource_jobs)==0 and len(resource_holder)==0:
            #         break
            #     for job_id in no_resource_jobs:
            #         if len(free_resources)==0:
            #             break
            #         metadata_lock.acquire()
            #         metadata.job_to_resources[job_id].append(free_resources.popleft())
            #         metadata_lock.release()
            #     if len(free_resources)==0:
            #         for job_id in resource_holder:
            #             if len(free_resources)==0:
            #                 break
            #             metadata_lock.acquire()
            #             metadata.job_to_resources[job_id].append(free_resources.popleft())
            #             metadata_lock.release()

            # metadata_lock.acquire()
            # resource_holders = [job_id for job_id in processing_job if len(metadata.job_to_resources[job_id])!=0]
            # no_resource_jobs = [job_id for job_id in processing_job if len(metadata.job_to_resources[job_id])==0]
            # max_query_rate_job_id = max(resource_holders, key= lambda x:  -math.inf if x not in metadata.job_dashboard else metadata.job_dashboard[x][1])
            # while len(no_resource_jobs)!=0 and len(metadata.job_to_resources[max_query_rate_job_id]) > 1:
            #     r = metadata.job_to_resources[max_query_rate_job_id].pop()
            #     job_id = no_resource_jobs.pop()
            #     metadata.job_to_resources[job_id].append(r)
            #     resource_holders.append(job_id)
            #     max_query_rate_job_id = max(resource_holders, key= lambda x:  -math.inf if x not in metadata.job_dashboard else metadata.job_dashboard[x][1])
            
            # max_query_rate_job_id = max(resource_holders, key= lambda x: -math.inf if x not in metadata.job_dashboard else metadata.job_dashboard[x][1])
            # min_query_rate_job_id = min(resource_holders, key= lambda x: math.inf if x not in metadata.job_dashboard else metadata.job_dashboard[x][1])
            # if max_query_rate_job_id in metadata.job_dashboard and min_query_rate_job_id in metadata.job_dashboard and metadata.job_dashboard[min_query_rate_job_id][1]!=0:
            #     diff_rate = (metadata.job_dashboard[max_query_rate_job_id][1] - metadata.job_dashboard[min_query_rate_job_id][1])/metadata.job_dashboard[min_query_rate_job_id][1]
            # else:
            #     diff_rate = math.inf
            # if  max_query_rate_job_id in metadata.job_dashboard and min_query_rate_job_id in metadata.job_dashboard and  diff_rate >= configs.load_balance_threshold:
            #     if len(metadata.job_to_resources[max_query_rate_job_id]) != 0:
            #         r = metadata.job_to_resources[max_query_rate_job_id].pop()
            #         metadata.job_to_resources[min_query_rate_job_id].append(r)
            # metadata_lock.release()

            # load balance 2
            metadata_lock.acquire()
            resources = [ip for host_name, ip, workspace_path, id in membership.get_membership_message() if ip != master_ip]
            for job_id in processing_job:
                if job_id not in metadata.job_to_resources:
                    metadata.job_to_resources[job_id] = []
                else:
                    for r in metadata.job_to_resources[job_id]:
                        if r not in resources:
                            metadata.job_to_resources[job_id].remove(r)
            free_resources = collections.deque()
            for r in resources:
                free = True
                for job_id, resource_list in metadata.job_to_resources.items():
                    if r in resource_list:
                        free = False
                        break
                if free:
                    free_resources.append(r)            
            # below only support two models
            no_resource_jobs = [job_id for job_id in processing_job if len(metadata.job_to_resources[job_id])==0]
            resource_holder =[job_id for job_id in processing_job if len(metadata.job_to_resources[job_id])!=0]
            metadata_lock.release()
            if len(no_resource_jobs)==2 and len(resource_holder)==0:
                a, b = no_resource_jobs[0], no_resource_jobs[1]
                x = 0
                for r in free_resources:
                    if x%2==0:
                        metadata.job_to_resources[a].append(r)
                    else:
                        metadata.job_to_resources[b].append(r)
                    x+=1
            elif len(no_resource_jobs)==1 and len(resource_holder)==1:
                a, b = no_resource_jobs[0], resource_holder[0]
                r_total = len(free_resources) + len(metadata.job_to_resources[resource_holder[0]])
                for r in free_resources:
                    if len(metadata.job_to_resources[a]) < r_total//2:
                        metadata.job_to_resources[a].append(r)
                    else:
                        metadata.job_to_resources[b].append(r)
                while len(metadata.job_to_resources[a]) < r_total//2:
                    tmp_r = metadata.job_to_resources[b].pop()
                    metadata.job_to_resources[a].append(tmp_r)
            elif len(no_resource_jobs)==0 and len(resource_holder)==2:
                if resource_holder[0] not in metadata.job_dashboard or resource_holder[1] not in metadata.job_dashboard:
                    continue
                a, b = min(resource_holder, key= lambda x:metadata.job_dashboard[x][1]), max(resource_holder, key= lambda x:metadata.job_dashboard[x][1])
                if metadata.job_dashboard[a] !=0 and (metadata.job_dashboard[b] - metadata.job_dashboard[a] / metadata.job_dashboard[a]) >= configs.load_balance_threshold:
                    tmp_r = metadata.job_to_resources[b].pop()
                    metadata.job_to_resources[a].append(tmp_r)
                elif metadata.job_dashboard[a]:
                    tmp_r = metadata.job_to_resources[b].pop()
                    metadata.job_to_resources[a].append(tmp_r)
            elif len(no_resource_jobs)==1 or len(resource_holder)==1:
                if len(no_resource_jobs)==1:
                    a = no_resource_jobs[0]
                else:
                    a = resource_holder[0]
                for r in free_resources:
                    metadata.job_to_resources[a].append(r)


        elif metadata.job_phase == utils.Job_Phase.TRAINING:
            pass

        # print(metadata.model_to_job_queue)
        # print(metadata.job_info)
        # print(metadata.job_dashboard)
        # print(metadata.job_to_resources)


def job_dispatcher(job_id):
    global t_master_should_stop, metadata_lock, metadata
    metadata_lock.acquire()
    status, model, input_dataset_file_name, _type = metadata.job_info[job_id]
    metadata_lock.release()
    # communicator.read_from_SDFS(globals.pwd+"/local_dir/input_job" + job_id + "_" + model +".txt.working", input_dataset_file_name, 1)
    time.sleep(30)
    f_input = open(globals.pwd+"/local_dir/" + input_dataset_file_name, "r")
    next(f_input)
    counter = 0
    for line in f_input:
        counter += 1
    metadata_lock.acquire()
    metadata.job_max_query_cnt[job_id] = counter//metadata.batch_size +1
    metadata_lock.release()
    f_input.close()
    time.sleep(2)
    f_input = open(globals.pwd+"/local_dir/" + input_dataset_file_name, "r")
    try:
        # skip first line (schema)
        next(f_input)
        # skip processed lines if some queries processed.

        if job_id in metadata.job_dashboard:
            for i in range(metadata.job_dashboard[job_id][4]):
                next(f_input)
            query_id = metadata.job_dashboard[job_id][2]
    except:
        pass
    batch_size = int(metadata.batch_size)
    query_id = 0
    cnt = 0
    for line in f_input:
        if t_master_should_stop or globals.host_status != utils.Status.ACTIVE or metadata.job_phase != utils.Job_Phase.INFERENCE or len(metadata.model_to_job_queue[model]) == 0 or job_id != metadata.model_to_job_queue[model][0]:
            return
        if cnt == batch_size:
            metadata_lock.acquire()
            dispatch_job(job_id, query_id, cnt)
            if job_id not in metadata.job_to_query_inputs:
                metadata.job_to_query_inputs[job_id] = collections.deque()
            metadata.job_to_query_inputs[job_id].append(cnt)
            metadata_lock.release()
            query_id += 1
            cnt = 0
            time.sleep(configs.dispatch_speed)
        cnt += 1
        # print("into job_dispatcher 4")


    if cnt != 0:
        metadata_lock.acquire()
        dispatch_job(job_id, query_id, cnt)
        if job_id not in metadata.job_to_query_inputs:
            metadata.job_to_query_inputs[job_id] = collections.deque()
        metadata.job_to_query_inputs[job_id].append(cnt)
        metadata_lock.release()
        query_id += 1
        cnt = 0
    # os.remove(globals.pwd+"/sdfs_dir/input_job "_" + model +".txt.working")
    f_input.close()
    # print("into job_dispatcher 6")

def redispatcher(job_id):
    global metadata, metadata_lock
    while not t_master_should_stop and globals.host_status == utils.Status.ACTIVE and metadata.job_phase == utils.Job_Phase.INFERENCE:
        time.sleep(25)
        # resend lost query
        metadata_lock.acquire()
        if job_id not in metadata.job_dashboard or job_id not in metadata.job_to_query_outputs:
            metadata_lock.release()
            continue
        accuracy, query_rate, query_processed_cnt, total_correct_num, total_data_num = metadata.job_dashboard[job_id]
        if len(metadata.job_to_query_outputs[job_id])>0:
            for i, output in enumerate(metadata.job_to_query_outputs[job_id]):
                if output == None and i < len(metadata.job_to_query_inputs[job_id]) and metadata.job_to_query_inputs[job_id][i]!=None:
                    dispatch_job(job_id, query_processed_cnt + i, metadata.job_to_query_inputs[job_id][i])
        if metadata.job_info[job_id][0] == utils.Job_Status.FINISH:
            metadata_lock.release()
            break
        metadata_lock.release()

def dispatch_job(job_id, query_id, cnt):
    global metadata, metadata_lock
    # print("into dispatch_job 1")
    target_resource_ip = min(metadata.job_to_resources[job_id], key= lambda x: 0 if x not in metadata.resource_to_queued_query_cnt else metadata.resource_to_queued_query_cnt[x])
    if target_resource_ip not in metadata.resource_to_queued_query_cnt:
        metadata.resource_to_queued_query_cnt[target_resource_ip] = 0
    metadata.resource_to_queued_query_cnt[target_resource_ip] += 1
    dispatch_msg = [utils.MessageType.DISPATCH, utils.get_host_ip(), 1, (job_id, metadata.job_info[job_id], query_id, cnt, metadata.batch_size)]
    # print("dispatch msg", dispatch_msg)
    communicator.udp_send(dispatch_msg, target_resource_ip, configs.listener_port)
    # print("into dispatch_job 7")

def query_reply_receiver():
    global t_master_should_stop, metadata_lock, metadata
    counter = 0
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((utils.get_host_ip(), configs.query_reply_receiver_port))
    logger.info('listener program started')
    while not t_master_should_stop and globals.host_status == utils.Status.ACTIVE and metadata.job_phase == utils.Job_Phase.INFERENCE:
        try:
            # print("wait message in rcv")
            data, addr = s.recvfrom(4096)
            logger.info("connection from: " + str(addr) + " with data: " + data.decode())
            if data:
                # print("data")
                # get message
                message =json.loads(data.decode())
                message_type = message[0]
                sender_ip = message[1]
                TTL = message[2]
                payload = message[3]
                # update timestamp
                failure_detector.set_timestamp(sender_ip, time.time())
                # skip message from non-member
                if not membership.is_member(sender_ip):
                    continue
                # check TTL
                if TTL == 0:
                    continue
                # print(message)
                counter+=1
                if counter%50==0:
                    print("Safe shut down period start.\n")
                    gc.collect()
                    time.sleep(10)
                    print("Safe shut down period end.\n")
                
                if message_type == utils.MessageType.ACK:
                    # print(message)
                    # extract data
                    job_id, model, query_id, result = payload
                    batch_num, correct_num, processing_time, output = result
                    metadata_lock.acquire()
                    metadata.resource_to_queued_query_cnt[sender_ip] -=1
                    
                    # inintialize data
                    if job_id not in metadata.job_dashboard:
                        metadata.job_dashboard[job_id] = [None, 0, 0, 0, 0]
                    if job_id not in metadata.job_to_query_outputs:
                        metadata.job_to_query_outputs[job_id] = collections.deque()
                    if job_id not in metadata.job_to_query_replys:
                        metadata.job_to_query_replys[job_id] = []
                    
                    # check reply
                    accuracy, query_rate, query_processed_cnt, total_correct_num, total_data_num = metadata.job_dashboard[job_id]
                    if query_id < query_processed_cnt or metadata.job_info[job_id][0] != utils.Job_Status.PROCESSING:
                        # print(query_id, query_processed_cnt, metadata.job_info[job_id][0])
                        metadata_lock.release()
                        continue

                    # put output to list
                    # print(query_processed_cnt, len(metadata.job_to_query_outputs[job_id]), query_id)
                    if query_processed_cnt + len(metadata.job_to_query_outputs[job_id]) <= query_id:
                        cnt = query_id - (query_processed_cnt + len(metadata.job_to_query_outputs[job_id]))
                        for i in range(cnt):
                            metadata.job_to_query_outputs[job_id].append(None)
                        metadata.job_to_query_outputs[job_id].append(output)
                    else:
                        if metadata.job_to_query_outputs[job_id][query_id-query_processed_cnt] == None:
                            metadata.job_to_query_outputs[job_id][query_id-query_processed_cnt] = output
                    # print(metadata.job_to_query_outputs[job_id])

                    # start from head of deque to append data to file
                    while len(metadata.job_to_query_outputs[job_id]) !=0 and metadata.job_to_query_outputs[job_id][0]!= None:
                        # print("out of queue", metadata.job_to_query_outputs[job_id][0])
                        tmp_f = metadata.job_to_output_opend_file[job_id]
                        tmp_output = metadata.job_to_query_outputs[job_id].popleft()
                        metadata.job_to_query_inputs[job_id].popleft()
                        query_processed_cnt += 1
                        for line in tmp_output:
                            tmp_f.write("%s\n" % line)

                    # print(metadata.job_to_query_outputs[job_id], metadata.job_dashboard[job_id][2])
                    # reset timer
                    metadata.job_to_time[job_id] = [time.time(), metadata.job_to_time[job_id][1] + (time.time()-metadata.job_to_time[job_id][0])]

                    # put metadata to list
                    if len(metadata.job_to_query_replys[job_id]) <= query_id:
                        cnt = query_id - len(metadata.job_to_query_replys[job_id])
                        for i in range(cnt):
                            metadata.job_to_query_replys[job_id].append(None)
                        metadata.job_to_query_replys[job_id].append((batch_num, correct_num, processing_time))
                    else:
                        metadata.job_to_query_replys[job_id][query_id] = (batch_num, correct_num, processing_time)
                    # print(metadata.job_to_query_replys[job_id])

                    # calculate metadata
                    total_correct_num = sum([x[1] for x in metadata.job_to_query_replys[job_id][:query_processed_cnt] if x != None])
                    total_data_num = sum([x[0] for x in metadata.job_to_query_replys[job_id][:query_processed_cnt] if x != None])
                    metadata.job_dashboard[job_id] = ["?" if total_data_num==0 else total_correct_num/total_data_num, 0 if metadata.job_to_time[job_id][1]==0 else query_processed_cnt/metadata.job_to_time[job_id][1], query_processed_cnt, total_correct_num, total_data_num]
                    
                    # print(metadata.job_dashboard[job_id])
                    # end of a job
                    if job_id in metadata.job_max_query_cnt and metadata.job_dashboard[job_id][2] == metadata.job_max_query_cnt[job_id]:
                        processing_time = map(lambda x:x[2], metadata.job_to_query_replys[job_id])
                        metadata.job_stat[job_id] = (statistics.mean(processing_time), statistics.stdev(processing_time), statistics.median(processing_time), np.percentile(processing_time, 90), np.percentile(processing_time, 95), np.percentile(processing_time, 99))
                        
                        metadata.resource_to_queued_query_cnt.pop(job_id)
                        metadata.job_to_output_opend_file[job_id].close()
                        metadata.job_to_output_opend_file.pop(job_id)
                        metadata.job_info[job_id][0] = utils.Job_Status.FINISH
                        metadata.job_to_resources.pop(job_id)
                        metadata.job_to_query_inputs.pop(job_id)
                        metadata.job_to_query_outputs.pop(job_id)

                    metadata_lock.release()

        except Exception as e:
            metadata_lock.release()
            print(traceback.format_exc())
            print(e)

def master_ack_receiver(expect_ack_num):
    global ack_list
    logger.info("master ack receiver started")
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    local_port = configs.master_ack_receiver_port
    while True:
        try:
            s.bind((utils.get_host_ip(), local_port))
            break
        except:
            local_port+=1
            pass
    
    s.settimeout(configs.ack_timeout)
    # leave or t_master_should_stop would cause master() finish
    while (not t_master_should_stop and globals.host_status == utils.Status.ACTIVE) and (len(ack_list) < expect_ack_num):
        try:
            data, addr = s.recvfrom(4096)
            logger.info("connection from: " + str(addr) + " with data: " + data.decode())
            if data:
                # get message
                message =json.loads(data.decode())
                message_type = message[0]
                sender_ip = message[1]
                TTL = message[2]
                payload = message[3]
                # print("ack msg From datanode IN master ack receiver func: ", message)
                # print("From", (addr))
                # update timestamp
                failure_detector.set_timestamp(sender_ip, time.time())
                # print("after update timestamp")
                # skip message from non-member
                if not membership.is_member(sender_ip):
                    continue
                # print("after skip message from non-member")
                # check TTL
                if TTL == 0:
                    continue
                # print("after check TTL")
                if message_type== utils.MessageType.ACK:
                    sdfs_file_name = payload[0]
                    newest_version = payload[1]
                    ack_list.append((sender_ip, sdfs_file_name, newest_version))
                # print("after append list")
                # print(ack_list)
        except socket.timeout:
            logger.info("socket listen from datanode timeout")
            print("socket listen from datanode timeout")
    # print("Finish ack recver")
    s.close()

def master_monitor():
    global metadata
    try:
        logger.info("master_monitor started")
        while not t_master_should_stop and globals.host_status == utils.Status.ACTIVE:
            time.sleep(configs.re_replica_period)
            # check
            affected_files = []
            down_ip = []
            # print("acquire lock 5")
            metadata_lock.acquire()
            for ip, files in metadata.ip_to_files.items():
                if not membership.is_member(ip):
                    affected_files.extend(files)
                    down_ip.append(ip)
            # print("aaaaaaaaaaa")
            metadata_lock.release()
            # print("release lock 5")
            if len(down_ip)==0:
                continue
            # update metadata.ip_to_files
            for ip in down_ip:
                del metadata.ip_to_files[ip]
            
            # update metadata.file_to_ips
            for file in affected_files:
                for ip in down_ip:
                    # print("acquire lock 6")
                    metadata_lock.acquire()
                    if ip in metadata.file_to_ips[file]:
                        metadata.file_to_ips[file].remove(ip)
                    metadata_lock.release()
                    # print("release lock 6")
            # print("acquire lock 7")
            metadata_lock.acquire()
            # allocate and re-replica for each file
            for file in affected_files:
                
                new_location = file_locations_allocate(metadata, 4-len(metadata.file_to_ips[file]), metadata.file_to_ips[file])
                # update metadata
                metadata.file_to_ips[file].extend(new_location)
                for ip in new_location:
                    if ip not in metadata.ip_to_files:
                        metadata.ip_to_files[ip] = []
                    metadata.ip_to_files[ip].append(file)
                # request for writing to datanode
                ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                if len(metadata.file_to_ips[file]) > 0:
                    request_re_replica = [utils.MessageType.REQ_REPLICATE, utils.get_host_ip(), 1, (file, new_location, metadata.file_version[file])]
                    ss.sendto(json.dumps(request_re_replica).encode(), (metadata.file_to_ips[file][0], configs.listener_port)) # at least one ip have file
                ss.close()
            metadata_lock.release()
            # print("release lock 7")
    except:
        pass

def send_all_files(sdfs_file_name, locations_list, file_version):
    """
    input is sdfs_file_name, locations_list
    send all version of sdfs_file_name to locations in list
    """
    for i in range(1, file_version+1):
        # send file to next neighbor
        
        sdfsfilename_with_version = sdfs_file_name+"."+str(i)
        # error handling
        if len(locations_list)==0 or membership.get_node_info(locations_list[0])== None:
            return
        t_remote_wirte_file = threading.Thread(target=remote_wirte_file, args=(locations_list[0], configs.sdfs_path + sdfsfilename_with_version, membership.get_node_info(locations_list[0])[2] + "/sdfs_dir/" + sdfsfilename_with_version))
        t_remote_wirte_file.start()
        t_remote_wirte_file.join()

        # send DATA_WRITE to neighbor
        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        data_write_msg = [utils.MessageType.DATA_WRITE, utils.get_host_ip(), len(locations_list), (sdfsfilename_with_version, locations_list)]
        ss.sendto(json.dumps(data_write_msg).encode(), (locations_list[0],configs.listener_port))

def file_locations_allocate(metadata_: Metadata, required_location_num=4, exist_location=[]):
    """
    input metadata, file_name
    return locations (1-4 location, depend on number of machine in system)
    """
    membership_ip_list= map(lambda x:x[1], membership.get_membership_message())
    ip_to_file_count = {}
    for ip in membership_ip_list:
        ip_to_file_count[ip] = 0
        if ip in metadata_.ip_to_files:
            for file in metadata_.ip_to_files[ip]:
                ip_to_file_count[ip] += metadata_.file_version[file]
    # sort ip by its holding files count
    sorted_ip = list(map(lambda x:x[0], sorted(list(ip_to_file_count.items()), key= lambda x: x[1])))
    new_location = []
    for ip in sorted_ip:
        if len(new_location) == required_location_num:
            break
        if ip not in exist_location:
            new_location.append(ip)
    return new_location

def log_clear():
    """
    for initialization when program start
    """
    files = [f for f in os.listdir(configs.log_path) if os.path.isfile(f)]

    try:
        for f in files:
            file_path =configs.log_path+f
            os.remove(file_path)
    except OSError as e:
        logger.error("Error: %s : %s" % (file_path, e.strerror))
        print("log clear error")
    return

def sdfs_clear():
    """
    for initialization when new member join or program start 
    """
    files = os.listdir(configs.sdfs_path)

    try:
        for f in files:
            file_path =configs.sdfs_path+f
            os.remove(file_path)
    except OSError as e:
        logger.error("Error: %s : %s" % (file_path, e.strerror))
        print("sdfs clear error")
    return

def list_all_sdfs_files():
    """
    no input
    return all files in sdfs_dir
    """
    files = os.listdir(configs.sdfs_path)
    return list(set([".".join(f.split('.')[:-1]) for f in files if f.split('.')[-1].isnumeric()])) 

def sdfs_delete_file(file_name, file_version):
    """
    input is file_name without version.
    This function would delete all versions of file from sdfs_dir. 
    decode by json.loads(data.decode())
    """
    files = [file_name+"."+str(ver) for ver in range(1, file_version+1)]
    try:
        for f in files:
            file_path =configs.sdfs_path+f
            os.remove(file_path)
    except OSError as e:
        logger.error("Error: %s : %s" % (file_path, e.strerror))
        print("Error: %s : %s" % (file_path, e.strerror))
    return

def remote_wirte_file(ip, local_path, remote_path):
    """
    move file from local server local_path to remote server (ip, host) remote_path.
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=ip, 
                username=globals.username,
                password=globals.password)

    # SCPCLient takes a paramiko transport as its only argument
    scpClient = scp.SCPClient(ssh.get_transport())

    scpClient.put(local_path, remote_path)

    scpClient.close()

def remote_read_file(ip, local_path, remote_path):
    """
    move file from remote server (ip, host) remote_path to local server local_path.
    """
    ssh = paramiko.SSHClient()

    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=ip, 
                username=globals.username,
                password=globals.password)
    
    # SCPCLient takes a paramiko transport as its only argument
    scpClient = scp.SCPClient(ssh.get_transport())

    scpClient.get(remote_path, local_path)

    scpClient.close()
