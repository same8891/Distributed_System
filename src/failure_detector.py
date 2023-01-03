import socket
import threading
import time
import utils
import configs
import logging
import globals
import json
import random
import membership
import communicator
import file_controller

def init():
    global logger, process_pong_timestamp, process_pong_timestamp_lock
    logger = logging.getLogger('failure_detector')
    process_pong_timestamp = {}
    process_pong_timestamp_lock = threading.Lock()

def set_timestamp(ip, timestamp):
    """
    update timestamp
    input ip & timestamp
    """
    global process_pong_timestamp_lock, process_pong_timestamp
    process_pong_timestamp_lock.acquire()

    process_pong_timestamp[ip] = timestamp

    process_pong_timestamp_lock.release()

def pinger():
    """
    Send PING to current process's neighbor using UDP. If the host is leaved/failed, then do nothing.
    3 neighbor: 1 is prev one, 2 is next one, 3 is next next one
    return: None
    """
    global logger, process_pong_timestamp, process_pong_timestamp_lock 
    logger.info("sender started")
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while True:
        time.sleep(configs.ping_period)
        if globals.host_status != utils.Status.ACTIVE:
            continue
        try:
            # not every time piggyback
            if random.randint(1, configs.piggyback_probablity) == 1:
                ping_msg = [utils.MessageType.PING, utils.get_host_ip(), 1, membership.get_membership_message()]
            else:
                ping_msg = [utils.MessageType.PING, utils.get_host_ip(), 1, None]

            # 1st neighbor (prev 1)
            n1 = membership.get_prev_member_info(utils.get_host_ip())
            if n1!=None:
                s.sendto(json.dumps(ping_msg).encode(), (n1[1], configs.listener_port))

            # 2nd neighbor (next 1)
            n2 = membership.get_next_member_info(utils.get_host_ip())
            if n2!=None:
                s.sendto(json.dumps(ping_msg).encode(), (n2[1], configs.listener_port))
                # 3rd neighbor (next 2)
                n3 = membership.get_next_member_info(n2[1])
                if n3!=None:
                    s.sendto(json.dumps(ping_msg).encode(), (n3[1], configs.listener_port))
        except Exception as e:
            print("pinger failure")
            print(e)

def monitor():
    """
    Monitor daemon that checks if any neighbor process has timeout

    return: None
    """
    global logger, process_pong_timestamp, process_pong_timestamp_lock
    logger.info("failure monitor started")
    last_time_store_membership = 0
    while True:
        time.sleep(configs.monitor_period)
        if globals.host_status != utils.Status.ACTIVE:
            continue
        # # every store period, update membership as a file into log_dir
        # if utils.get_host_ip() == configs.introducer_ip and time.time() - last_time_store_membership > configs.store_membership_period:
        #     t_datanode = threading.Thread(target=membership.store_membership)
        #     t_datanode.start()
        #     t_datanode.join()
        #     last_time_store_membership = time.time()

        process_pong_timestamp_lock.acquire()
        # find neighbor
        neighbor_ip_list = []
        # 1st neighbor (prev 1)
        n1 = membership.get_prev_member_info(utils.get_host_ip())
        if n1!=None:
            neighbor_ip_list.append(n1[1])

        # 2rd neighbor (next 1)
        n2 = membership.get_next_member_info(utils.get_host_ip())
        if n2!=None:
            neighbor_ip_list.append(n2[1])
            # next 3rd neighbor
            n3 = membership.get_next_member_info(n2[1])
            if n3!=None:
                neighbor_ip_list.append(n3[1])

        neighbor_ip_list = list(set(neighbor_ip_list))
        # remain neighbors pong timestamp
        new_process_pong_timestamp = {}
        for ip in neighbor_ip_list:
            if ip in process_pong_timestamp:
                new_process_pong_timestamp[ip] = process_pong_timestamp[ip]
            else:
                new_process_pong_timestamp[ip] = time.time()
        process_pong_timestamp = new_process_pong_timestamp

        fail_members_ip = []
        # check timeout
        for ip in neighbor_ip_list:
            if time.time() - process_pong_timestamp[ip] > configs.fail_timeout:
                fail_members_ip.append(ip)
        
        # update membership
        logger.info("Encounter timeout before:\n")
        logger.info(json.dumps(membership.get_membership_message()))
        for mip in fail_members_ip:
            membership.delete_node(mip)
        logger.info("Encounter timeout after:\n")
        logger.info(json.dumps(membership.get_membership_message()))        

        # # send fail message to all members
        update_msg = [utils.MessageType.UPDATE, utils.get_host_ip(), 1, membership.get_membership_message()]
        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for host_name, ip, workspace_path, id in membership.get_membership_message():
            if ip != utils.get_host_ip():
                ss.sendto(json.dumps(update_msg).encode(), (ip, configs.listener_port))

        process_pong_timestamp_lock.release()

        # detect master failed
        if not membership.is_member(file_controller.master_ip):
            communicator.invoke_election()
