import imp
import socket
import threading
import time
import utils
import configs
import logging
import globals
import json
import traceback
import os

import membership
import failure_detector
import file_controller
import machine_learning


def init():
    global logger
    logger = logging.getLogger('communicator')

def invoke_election():
    """
    call this function would invoke election from current client
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    neighbor = membership.get_next_member_info(utils.get_host_ip())
    host_info = membership.get_node_info(utils.get_host_ip())
    election_msg = [utils.MessageType.ELECTION, utils.get_host_ip(), 1, host_info]
    if neighbor:
        s.sendto(json.dumps(election_msg).encode(), (neighbor[1], configs.listener_port))
    else:
        s.sendto(json.dumps(election_msg).encode(), (utils.get_host_ip(), configs.listener_port))
    s.close()

def join():
    """
    request join to introducer
    """
    logger.info("start joining")
    ## initialize
    # leave()
    file_controller.sdfs_clear()
    membership.clear()
    # update status
    globals.host_status = utils.Status.ACTIVE

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if utils.get_host_ip() != globals.introducer_ip:
        join_msg = [utils.MessageType.JOIN, utils.get_host_ip(), 1, (utils.get_host_name(), utils.get_host_ip(), globals.pwd)]
        s.sendto(json.dumps(join_msg).encode(), (globals.introducer_ip, configs.listener_port))
    else:
        #if membership.restore_membership():
        #   # send join message to others
        #   update_msg = [utils.MessageType.UPDATE, utils.get_host_ip(), 1, membership.get_membership_message()]
        #   ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        #   for host_name, ip, id in membership.get_membership_message():
        #       if ip != utils.get_host_ip():
        #           ss.sendto(json.dumps(update_msg).encode(), (ip, configs.listener_port))
        #   ss.close()
        #else:
        membership.insert_node(utils.get_host_name(),utils.get_host_ip(),globals.pwd)

def leave():
    """
    leave membership (other would detect me as timeout)
    change status make our thread stop working, but easily restore by JOIN.
    """
    logger.info("start leave")
    globals.host_status = utils.Status.LEAVE
    file_controller.sdfs_clear()
    file_controller.log_clear()
    membership.clear()

def update_introducer(ip):
    globals.introducer_ip = ip
    for host_name, ip, workspace_path, id in membership.get_membership_message():
        if utils.get_host_ip() == ip:
            continue
        request_update_intro_msg = [utils.MessageType.UPDATE_INTRO, utils.get_host_ip(), 1, ip]
        udp_send(request_update_intro_msg, ip, configs.listener_port)

def write_to_SDFS(file_path, sdfs_file_name):
    """
    use for put, (write output file to SDFS)
    put {file_path} from local dir to remote SDFS as {sdfs_file_name}
    return success or fail string.
    """
    if not membership.is_member(file_controller.master_ip):
        logger.info("Failed, master downed.")
        return "Failed, master downed."
    if not os.path.exists(file_path):
        return "Local file not exist."
    s = socket.socket(socket.AF_INET, socket.SO_REUSEADDR)
    # send REQ_PUT with sdfs_filename via UDP to Master's master_port
    request_put_msg = [utils.MessageType.REQ_PUT, utils.get_host_ip(), 1, sdfs_file_name]
    # print("req put msg To master IN request_put func: ", request_put_msg)
    # print("To: ", (file_controller.master_ip, configs.master_port))
    s.sendto(json.dumps(request_put_msg).encode(), (file_controller.master_ip, configs.master_port))
    s.close()
    
    s = socket.socket(socket.AF_INET, socket.SO_REUSEADDR)
    s.bind((utils.get_host_name(), configs.request_ack_port))
    # receive ACK via UDP with locations_list from Master
    try:
        s.settimeout(configs.ack_timeout)
        data, addr = s.recvfrom(4096)
        logger.info("connection from: " + str(addr) + " with data: " + data.decode())
        if data:
            # get message
            message =json.loads(data.decode())
            message_type = message[0]
            sender_ip = message[1]
            TTL = message[2]
            payload = message[3] # payload is a list of where should file stores.
            # print("ack msg FROM master IN request_put func: ", message)
            # print("From: ", (addr))
            locations_list = payload[0]
            newest_version = payload[1] # not yet exist
        else:
            logger.info("master reply no data")
            s.close()
            return "Unexpected Error."

    except socket.timeout:
        logger.info("socket listen from master timeout")
        return "Master reply timeout."
    finally:
        s.close()

    # send file to next neighbor
    
    sdfsfilename_with_version = sdfs_file_name+"."+str(newest_version)
    t_remote_wirte_file = threading.Thread(target=file_controller.remote_wirte_file, args=(locations_list[0], file_path, membership.get_node_info(locations_list[0])[2] + "/sdfs_dir/" + sdfsfilename_with_version))
    t_remote_wirte_file.start()
    t_remote_wirte_file.join()
    time.sleep(1)
    # send DATA_WRITE to neighbor
    ss = socket.socket(socket.AF_INET, socket.SO_REUSEADDR)
    vdata_write_msg = [utils.MessageType.DATA_WRITE, utils.get_host_ip(), len(locations_list)-1, (sdfsfilename_with_version, locations_list)]
    ss.sendto(json.dumps(vdata_write_msg).encode(), (locations_list[0],configs.listener_port))    

    return "Successfully write to other datanodes."

def read_from_SDFS(file_path, sdfs_file_name, version_num):
    """
    use for get, get-versions,  (read file to master)
    get {sdfsfilenamefrom} from remote sdfs dir as {file_path} 
    return success or fail string.
    """
    logger.info("into request put functions")
    if not membership.is_member(file_controller.master_ip):
        logger.info("Failed, master downed.")
        return "Failed, master downed."

    s = socket.socket(socket.AF_INET, socket.SO_REUSEADDR)
    # send REQ_GET with sdfs_filename via UDP to Master's master_port
    request_get_msg = [utils.MessageType.REQ_GET, utils.get_host_ip(), 1, (file_path, sdfs_file_name, version_num)]
    # print("request_get_msg To master IN listener func: ", request_get_msg)
    # print("To: ", (file_controller.master_ip, configs.master_port))
    s.sendto(json.dumps(request_get_msg).encode(), (file_controller.master_ip, configs.master_port))
    s.close()
    
    s = socket.socket(socket.AF_INET, socket.SO_REUSEADDR)
    s.bind((utils.get_host_ip(), configs.request_ack_port))
    # receive ACK via UDP with locations_list from Master
    try:
        s.settimeout(configs.ack_timeout)
        data, addr = s.recvfrom(4096)
        logger.info("connection from: " + str(addr) + " with data: " + data.decode())
        if data:
            # get message
            message =json.loads(data.decode())
            message_type = message[0]
            sender_ip = message[1]
            TTL = message[2]
            payload = message[3] # payload is a list of where should file stores.
            if message_type == utils.MessageType.ERROR_ACK:
                return "SDFS File Name donesn't exist."

        else:
            logger.info("master reply no data")
            s.close()
            return "Unexpected Error."

    except socket.timeout:
        logger.info("socket listen from master timeout")
        return "Master reply timeout."
    finally:
        s.close()
    return "Successfully read from other datanodes."
    
def request_delete(sdfs_file_name):
    """
    delete {sdfs_file_name} in system
    return success or fail string.
    """
    logger.info("into request delete functions")
    if not membership.is_member(file_controller.master_ip):
        logger.info("Failed, master downed.")
        return "Failed, master downed."

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # send REQ_DELETE with sdfs_filename via UDP to Master's master_port
    request_delete_msg = [utils.MessageType.REQ_DELETE, utils.get_host_ip(), 1, sdfs_file_name]
    s.sendto(json.dumps(request_delete_msg).encode(), (file_controller.master_ip, configs.master_port))
    return "Successfully delete file or file doesn't exist."

def request_list(sdfs_file_name):
    """
    ls {sdfs_file_name} in system
    return list of ips who holds sdfs_file_names.
    """
    logger.info("into request delete functions")
    if not membership.is_member(file_controller.master_ip):
        logger.info("Failed, master downed.")
        return "Failed, master downed."

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # send REQ_LIST with sdfs_filename via UDP to Master's master_port
    request_list_msg = [utils.MessageType.REQ_LIST, utils.get_host_ip(), 1, sdfs_file_name]
    s.sendto(json.dumps(request_list_msg).encode(), (file_controller.master_ip, configs.master_port))
    s.close()

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((utils.get_host_ip(), configs.request_ack_port))
    # receive ACK via UDP with locations_list from Master
    try:
        s.settimeout(configs.ack_timeout)
        data, addr = s.recvfrom(4096)
        logger.info("connection from: " + str(addr) + " with data: " + data.decode())
        if data:
            # get message
            message =json.loads(data.decode())
            # print("get ls ack",message)
            message_type = message[0]
            sender_ip = message[1]
            TTL = message[2]
            payload = message[3] # payload is a list of where should file stores.
            if message_type == utils.MessageType.ERROR_ACK:
                return "SDFS File Name donesn't exist."
            locations_list = payload
        else:
            logger.info("master reply no data")
            s.close()
            return "Unexpected Error."

    except socket.timeout:
        logger.info("socket listen from master timeout")
        return "Master reply timeout."
    finally:
        s.close()
    
    return locations_list

def request_change_phase(new_phase):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # send CHANGE_PHASE with new_phase via UDP to Master's master_port
    request_change_phase_msg = [utils.MessageType.CHANGE_PHASE, utils.get_host_ip(), 1, new_phase]
    s.sendto(json.dumps(request_change_phase_msg).encode(), (file_controller.master_ip, configs.master_port))
    s.close()

    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss.bind((utils.get_host_ip(), configs.request_ack_port))
    # receive ACK via UDP from Master
    try:
        ss.settimeout(configs.ack_timeout)
        data, addr = ss.recvfrom(4096)
        logger.info("connection from: " + str(addr) + " with data: " + data.decode())
        if data:
            # get message
            message =json.loads(data.decode())
            message_type = message[0]
            sender_ip = message[1]
            TTL = message[2]
            payload = message[3]
            # extract payload
            message = payload
        else:
            logger.info("master reply no data")
            ss.close()
            return "Unexpected Error."

    except socket.timeout:
        logger.info("socket listen from master timeout")
        return "Master reply timeout."
    finally:
        ss.close()
    return message

def request_add_job(job_id, model, dataset_file_name):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # send CHANGE_PHASE with new_phase via UDP to Master's master_port
    request_add_job_msg = [utils.MessageType.ADD_JOB, utils.get_host_ip(), 1, (job_id, model, dataset_file_name)]
    s.sendto(json.dumps(request_add_job_msg).encode(), (file_controller.master_ip, configs.master_port))
    s.close()

    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss.bind((utils.get_host_ip(), configs.request_ack_port))
    # receive ACK via UDP from Master
    try:
        ss.settimeout(configs.ack_timeout)
        data, addr = ss.recvfrom(4096)
        logger.info("connection from: " + str(addr) + " with data: " + data.decode())
        if data:
            # get message
            message =json.loads(data.decode())
            message_type = message[0]
            sender_ip = message[1]
            TTL = message[2]
            payload = message[3]
            # extract payload
            message = payload
        else:
            logger.info("master reply no data")
            ss.close()
            return "Unexpected Error."

    except socket.timeout:
        logger.info("socket listen from master timeout")
        return "Master reply timeout."
    finally:
        ss.close()
    return message

def request_remove_job(job_id):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # send CHANGE_PHASE with new_phase via UDP to Master's master_port
    request_remove_job_msg = [utils.MessageType.REMOVE_JOB, utils.get_host_ip(), 1, job_id]
    s.sendto(json.dumps(request_remove_job_msg).encode(), (file_controller.master_ip, configs.master_port))
    s.close()

    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss.bind((utils.get_host_ip(), configs.request_ack_port))
    # receive ACK via UDP from Master
    try:
        ss.settimeout(configs.ack_timeout)
        data, addr = ss.recvfrom(4096)
        logger.info("connection from: " + str(addr) + " with data: " + data.decode())
        if data:
            # get message
            message =json.loads(data.decode())
            message_type = message[0]
            sender_ip = message[1]
            TTL = message[2]
            payload = message[3]
            # extract payload
            message = payload
        else:
            logger.info("master reply no data")
            ss.close()
            return "Unexpected Error."

    except socket.timeout:
        logger.info("socket listen from master timeout")
        return "Master reply timeout."
    finally:
        ss.close()
    return message

def request_set_batch_size(batch_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # send CHANGE_PHASE with new_phase via UDP to Master's master_port
    request_set_batch_size_msg = [utils.MessageType.SET_BATCH_SIZE, utils.get_host_ip(), 1, batch_size]
    s.sendto(json.dumps(request_set_batch_size_msg).encode(), (file_controller.master_ip, configs.master_port))
    s.close()

    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss.bind((utils.get_host_ip(), configs.request_ack_port))
    # receive ACK via UDP from Master
    try:
        ss.settimeout(configs.ack_timeout)
        data, addr = ss.recvfrom(4096)
        logger.info("connection from: " + str(addr) + " with data: " + data.decode())
        if data:
            # get message
            message =json.loads(data.decode())
            message_type = message[0]
            sender_ip = message[1]
            TTL = message[2]
            payload = message[3]
            # extract payload
            message = payload
        else:
            logger.info("master reply no data")
            ss.close()
            return "Unexpected Error."

    except socket.timeout:
        logger.info("socket listen from master timeout")
        return "Master reply timeout."
    finally:
        ss.close()
    return message

def request_query_dashboard(job_id):
    request_query_dashboard_msg = [utils.MessageType.REQUEST_QUERY_DASHBOARD, utils.get_host_ip(), 1, job_id]
    udp_send(request_query_dashboard_msg, file_controller.master_ip, configs.master_port)
    
    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss.bind((utils.get_host_ip(), configs.request_ack_port))
    # receive ACK via UDP from Master
    try:
        ss.settimeout(configs.ack_timeout)
        data, addr = ss.recvfrom(4096)
        logger.info("connection from: " + str(addr) + " with data: " + data.decode())
        if data:
            # get message
            message =json.loads(data.decode())
            message_type = message[0]
            sender_ip = message[1]
            TTL = message[2]
            payload = message[3]
            # extract payload
            message = payload
        else:
            logger.info("master reply no data")
            ss.close()
            return "Unexpected Error."

    except socket.timeout:
        logger.info("socket listen from master timeout")
        return "Master reply timeout."
    finally:
        ss.close()
    return message

def request_query_stat(job_id):
    request_query_stat_msg = [utils.MessageType.REQUEST_QUERY_STAT, utils.get_host_ip(), 1, job_id]
    udp_send(request_query_stat_msg, file_controller.master_ip, configs.master_port)
    
    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss.bind((utils.get_host_ip(), configs.request_ack_port))
    # receive ACK via UDP from Master
    try:
        ss.settimeout(configs.ack_timeout)
        data, addr = ss.recvfrom(4096)
        logger.info("connection from: " + str(addr) + " with data: " + data.decode())
        if data:
            # get message
            message =json.loads(data.decode())
            message_type = message[0]
            sender_ip = message[1]
            TTL = message[2]
            payload = message[3]
            # extract payload
            message = payload
        else:
            logger.info("master reply no data")
            ss.close()
            return "Unexpected Error."

    except socket.timeout:
        logger.info("socket listen from master timeout")
        return "Master reply timeout."
    finally:
        ss.close()
    return message

def request_show_result(job_id, line_interval_start, line_interval_end):
    request_show_result_msg = [utils.MessageType.REQUEST_JOB_RESULT, utils.get_host_ip(), 1, job_id]
    udp_send(request_show_result_msg, file_controller.master_ip, configs.master_port)

    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss.bind((utils.get_host_ip(), configs.request_ack_port))
    # receive ACK via UDP from Master
    try:
        ss.settimeout(configs.ack_timeout)
        data, addr = ss.recvfrom(4096)
        logger.info("connection from: " + str(addr) + " with data: " + data.decode())
        if data:
            # get message
            message =json.loads(data.decode())
            message_type = message[0]
            sender_ip = message[1]
            TTL = message[2]
            payload = message[3]
            # extract payload
            message = payload
        else:
            logger.info("master reply no data")
            ss.close()
            return "Unexpected Error."

    except socket.timeout:
        logger.info("socket listen from master timeout")
        return "Master reply timeout."
    finally:
        ss.close()
    if type(message) == str:
        return message
    job_info = message
    file_name = "output_job" + job_id + "_" + job_info[1] + ".txt"
    location_list = request_list(file_name)
    data = []
    read_from_SDFS(globals.pwd+"/sdfs_dir/output_job" + job_id + "_" + job_info[1] +".txt.working", "output_job" + job_id + "_" + job_info[1] +".txt", 1)
    f = open(globals.pwd+"/sdfs_dir/output_job" + job_id + "_" + job_info[1] +".txt.working", "r")
    for i in range(line_interval_start):
        next(f)
    cnt = 0
    for line in f:
        if line_interval_end - line_interval_start + 1:
            break
        data.append(line)
        cnt+=1
    os.remove(configs.sdfs_path + "output_job" + job_id + "_" + job_info[1]+ ".txt.working")

    return (job_info, file_name, location_list, data)


def request_resource_dashboard():
    request_resource_dashboard_msg = [utils.MessageType.REQUEST_RESOURCE_DASHBOARD, utils.get_host_ip(), 1, None]
    udp_send(request_resource_dashboard_msg, file_controller.master_ip, configs.master_port)
    
    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss.bind((utils.get_host_ip(), configs.request_ack_port))
    # receive ACK via UDP from Master
    try:
        ss.settimeout(configs.ack_timeout)
        data, addr = ss.recvfrom(4096)
        logger.info("connection from: " + str(addr) + " with data: " + data.decode())
        if data:
            # get message
            message =json.loads(data.decode())
            message_type = message[0]
            sender_ip = message[1]
            TTL = message[2]
            payload = message[3]
            # extract payload
            message = payload
        else:
            logger.info("master reply no data")
            ss.close()
            return "Unexpected Error."

    except socket.timeout:
        logger.info("socket listen from master timeout")
        return "Master reply timeout."
    finally:
        ss.close()
    return message

def udp_send(msg, ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.sendto(json.dumps(msg).encode(), (ip, port))
    s.close()

def listener():
    """
    Run on every client.
    Handle ALL membership message. (4 types of messages)
    Besides, handle file operation request.
    """
    logger.info("listener started")
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((utils.get_host_ip(), configs.listener_port))
    logger.info('listener program started')
    while True:
        try:
            if globals.host_status != utils.Status.ACTIVE:
                logger.info("skip listener program since " + utils.get_host_name() + " is leaved")
                continue
            data, addr = s.recvfrom(4096)
            logger.info("connection from: " + str(addr) + " with data: " + data.decode())
            if data:
                # get message
                message =json.loads(data.decode())
                message_type = message[0]
                sender_ip = message[1]
                TTL = message[2]
                payload = message[3]
                # update timestamp
                failure_detector.set_timestamp(sender_ip, time.time())
                # skip sender from non-membership
                if message_type != utils.MessageType.JOIN and message_type != utils.MessageType.INTRO_UPDATE and not membership.is_member(sender_ip):
                    continue
                # check TTL
                if TTL == 0:
                    continue
                
                ########################
                # Membership message
                ########################
                if message_type == utils.MessageType.JOIN:
                    # receive join message, normally, only introducer would received this message
                    # if this host is introducer
                    if utils.get_host_ip() == globals.introducer_ip:
                        logger.info("introducer recv connection from new joiner: " + str(addr))
                        # insert node 
                        membership.insert_node(payload[0], payload[1],payload[2])
                        # print(membership.get_membership_message())
                        # send join message to others
                        intro_update_msg = [utils.MessageType.INTRO_UPDATE, utils.get_host_ip(), 1, (file_controller.master_ip, membership.get_membership_message())]
                        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        for host_name, ip, workspace_path,id in membership.get_membership_message():
                            if ip != utils.get_host_ip():
                                ss.sendto(json.dumps(intro_update_msg).encode(), (ip, configs.listener_port))
                        ss.close()
                    else:
                        pass

                elif message_type == utils.MessageType.PING:
                    # receive PING message. sometimes it is with membership payload
                    if payload!=None:
                        membership.membership_construct(payload)

                    # send pong
                    pong = [utils.MessageType.PONG, utils.get_host_ip(), 1, None]
                    s.sendto(json.dumps(pong).encode(), (sender_ip, configs.listener_port))          

                elif message_type == utils.MessageType.PONG:
                    # receive PONG message. used to record timestamp (done it in line 83), so do nothing
                    pass
                
                elif message_type == utils.MessageType.INTRO_UPDATE: # from fail or leave or join
                    # update membership
                    logger.info("Encounter UPDATE before:")
                    logger.info(json.dumps(membership.get_membership_message()))
                    file_controller.master_ip = payload[0]
                    membership.membership_construct(payload[1])
                    logger.info("Encounter UPDATE after:")
                    logger.info(json.dumps(membership.get_membership_message()))
                    
                elif message_type == utils.MessageType.UPDATE: # from fail or leave or join
                    # update membership
                    logger.info("Encounter UPDATE before:")
                    logger.info(json.dumps(membership.get_membership_message()))
                    membership.membership_construct(payload)
                    logger.info("Encounter UPDATE after:")
                    logger.info(json.dumps(membership.get_membership_message()))

                elif message_type == utils.MessageType.ELECTION:
                    # receive ELECTION message. start ring election protocol
                    file_controller.master_ip = payload[1]
                    host_info = membership.get_node_info(utils.get_host_ip())
                    if host_info[3] == payload[3]:
                        file_controller.t_master_should_stop = False
                        file_controller.master_ip = host_info[1]
                        file_controller.master_thread = threading.Thread(target=file_controller.master)
                        file_controller.master_thread.start()
                    else:
                        file_controller.t_master_should_stop = True
                        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        election_msg = [utils.MessageType.ELECTION, utils.get_host_ip(), 1, host_info if host_info[3] > payload[3] else payload]
                        file_controller.master_ip = host_info[1] if host_info[3] > payload[3] else payload[1]
                        neighbor = membership.get_next_member_info(utils.get_host_ip())
                        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        if neighbor:
                            ss.sendto(json.dumps(election_msg).encode(), (neighbor[1], configs.listener_port))
                        else:
                            ss.sendto(json.dumps(election_msg).encode(), (utils.get_host_ip(), configs.listener_port))
                        ss.close()
                ########################
                # datanode message
                ########################
                elif message_type == utils.MessageType.WRITE or message_type == utils.MessageType.READ:
                    # receive READ via UDP with sdfs_filename from client
                    # payload = sdfs_filename
                    # check version
                    # print("write msg FROM master IN listener func: ", message)
                    # print("From: ", (sender_ip))
                    # if payload not in file_controller.local_file_version:
                    #     newest_version = None
                    # else:
                    #     newest_version = file_controller.local_file_version[payload]
                    newest_version = None
                    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

                    # send ACK via UDP with (sdfs_filename, newest_version) to Master's master_ack_receiver_port
                    version_ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, (payload, newest_version)]
                    # print("version_ack_msg To master IN listener func: ", version_ack_msg)
                    # print("To: ", (file_controller.master_ip, configs.master_ack_receiver_port))
                    ss.sendto(json.dumps(version_ack_msg).encode(), (file_controller.master_ip, configs.master_ack_receiver_port))

                elif message_type == utils.MessageType.DELETE:
                    sdfs_file_name = payload[0]
                    version_num = payload[1]
                    file_controller.sdfs_delete_file(sdfs_file_name, version_num)

                elif message_type == utils.MessageType.REQ_REPLICATE:
                    sdfs_file_name = payload[0]
                    new_locations = payload[1]
                    file_version = payload[2]
                    
                    t_replicator = threading.Thread(target=file_controller.send_all_files, args=(sdfs_file_name, new_locations, file_version,))
                    t_replicator.start()
                
                elif message_type == utils.MessageType.DATA_WRITE:
                    # extract payload
                    sdfs_file_name_with_version = payload[0]
                    locations_list = payload[1]
                    t_datanode_for_data_write = threading.Thread(target=datanode_for_data_write, args=(sdfs_file_name_with_version, locations_list, TTL, payload))
                    t_datanode_for_data_write.start()                    
            
                elif  message_type == utils.MessageType.GET_DATA:
                    target_file_path = payload[0]
                    sdfs_file_name = payload[1]
                    target_location = payload[2]
                    version_interval = payload[3]             
                    t_datanode_for_get_data = threading.Thread(target=datanode_for_get_data, args=(target_file_path, sdfs_file_name, target_location, version_interval))
                    t_datanode_for_get_data.start() 

                elif  message_type == utils.MessageType.DISPATCH:
                    # print(message)
                    job_id, job_info, query_id, cnt, batch_size = payload
                    # print("receive dispatch message", message)
                    if job_info[3] == utils.Job_Type.INFERENCE:
                        t_inference = threading.Thread(target=machine_learning.inference, args=(job_id, job_info[1], query_id, cnt, batch_size))
                        t_inference.start() 
                    elif job_info[3] == utils.Job_Type.TRAINING:
                        # print("start training")
                        t_training = threading.Thread(target=machine_learning.training, args=(job_info[1],))
                        t_training.start() 
                
                ########################
                # datanode message
                ########################
                elif  message_type == utils.MessageType.UPDATE_INTRO:
                    globals.introducer_ip = payload
                    
                else:
                    logger.error("Unknown message type to listener")
        except Exception as e:
            print(traceback.format_exc())
            print(e)
        
def datanode_for_data_write(sdfs_file_name_with_version, locations_list, TTL, payload):
    """
    You should only call this function in listener
    """
    # send file to next neighbor
    location_len = len(locations_list)
    file_controller.remote_wirte_file(locations_list[location_len-TTL], configs.sdfs_path + sdfs_file_name_with_version, membership.get_node_info(locations_list[location_len-TTL])[2] + "/sdfs_dir/" + sdfs_file_name_with_version)

    # send DATA_WRITE to neighbor
    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    data_write_msg = [utils.MessageType.DATA_WRITE, utils.get_host_ip(), TTL-1, payload]
    location_len = len(locations_list)
    ss.sendto(json.dumps(data_write_msg).encode(), (locations_list[location_len-TTL],configs.listener_port))  

def datanode_for_get_data(target_file_path, sdfs_file_name, target_location, version_interval):
    """
    You should only call this function in listener
    """
    for i in range(version_interval[0], version_interval[1]+1):
        sdfsfilename_with_version = sdfs_file_name+"."+str(i)
        if version_interval[0] == version_interval[1]:
            target_file_path_with_version = target_file_path
        else:
            target_file_path_with_version = target_file_path + "." + str(i)
        file_controller.remote_wirte_file(target_location, configs.sdfs_path + sdfsfilename_with_version, target_file_path_with_version)
