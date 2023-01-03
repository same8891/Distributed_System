from distutils.command.config import config
import imp
import os
import signal
import socket
import time
import threading
import json
import logging
from logging.handlers import RotatingFileHandler
import datetime
import sys
import gc
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
os.environ["CUDA_VISIBLE_DEVICES"]='-1'
import globals
import configs
import utils
import log_querier
import membership
import failure_detector
import communicator
import file_controller
import machine_learning

def shell():
    """
    This function is a shell that allow user interact with this system.
    ANY user input and output handle in this function. 
    """
    print("Welcome to the interactive shell for Simple Distributed File System.", end='\n')
    
    time.sleep(1)
    # interactive shell
    while True:
        input_str = input("Wait for command...\n# ")
        
        if input_str == 'exit':
            os.kill(os.getpid(), signal.SIGINT)

        elif input_str[:4] == "grep":
            # grep and print output from log_querier
            output = log_querier.query(input_str)
            for line in output:
                print(line)

        elif input_str == "list_mem":
            # list membership list
            # print("=======================================", end='\n')
            print("=========== Membership List ===========", end='\n')
            print("|%-10s|%-40s|%-20s|" %("ID","Host Name","IP"), end='\n')
            for host_name, ip, workspace_path, id in membership.get_membership_message():
                print("|%-10s|%-40s|%-20s|" %(id,host_name,ip), end='\n')
            print("=======================================", end='\n')

        elif input_str == "list_self":
            # list member id
            self_node_info = membership.get_node_info(utils.get_host_ip())
            if not self_node_info:
                print("ID doesn't exist. Some error occurs.")
            else:
                print("ID: ", self_node_info[3], end='\n')
                
        elif input_str == "join":
            communicator.join()

        elif input_str == "leave":
            communicator.leave()

        elif input_str[:4] == "put ":
            cmd, local_file_name, sdfs_file_name = input_str.split()
            return_str = communicator.write_to_SDFS(globals.pwd+"/local_dir/"+local_file_name, sdfs_file_name)
            print(return_str, end='\n')
        
        elif input_str[:4] == "get ":
            cmd, sdfs_file_name, local_file_name = input_str.split()
            return_str = communicator.read_from_SDFS(globals.pwd+"/local_dir/"+local_file_name, sdfs_file_name, 1)
            print(return_str, end='\n')

        elif input_str[:7] == "delete ":
            cmd, sdfs_file_name = input_str.split()
            return_str = communicator.request_delete(sdfs_file_name)
            print(return_str, end='\n')

        elif input_str[:3] == "ls ":
            cmd, sdfs_file_name = input_str.split()
            return_val = communicator.request_list(sdfs_file_name)
            if type(return_val) is str:
                print(return_val)
            else:
                # get ips
                ip_list = return_val
                # get infos
                machines_info = []
                for ip in ip_list:
                    machines_info.append(membership.get_node_info(ip))
                # print
                print("=========== Machines who holds this file ===========", end='\n')
                print("|%-10s|%-40s|%-20s|" %("ID","Host Name","IP"), end='\n')
                for host_name, ip, workspace_path, id in machines_info:
                    print("|%-10s|%-40s|%-20s|" %(id,host_name,ip), end='\n')
                print("====================================================", end='\n')

        elif input_str == "store":
            # get files
            files_list = file_controller.list_all_sdfs_files()
            # print
            print("=========== Files store in SDFS ===========", end='\n')
            print("|%-40s|" %("File Name"), end='\n')
            for file_name in files_list:
                print("|%-40s|" %(file_name), end='\n')
            print("===========================================", end='\n')

        elif input_str[:13] == "get-versions ":
            cmd, sdfs_file_name, numversions, local_file_name = input_str.split()
            return_str = communicator.read_from_SDFS(globals.pwd+"/local_dir/"+local_file_name, sdfs_file_name, int(numversions))
            print(return_str, end='\n')

        elif input_str == "t-start":
            message = communicator.request_change_phase(utils.Job_Phase.TRAINING)
            print(message, end='\n')

        elif input_str == "i-start":
            message = communicator.request_change_phase(utils.Job_Phase.INFERENCE)
            print(message, end='\n')

        elif input_str == "end":
            message = communicator.request_change_phase(utils.Job_Phase.NONE)
            print(message, end='\n')

        elif input_str[:4] == "add ":
            cmd, job_id, model, dataset_file_name = input_str.split()
            print(model, dataset_file_name)
            message = communicator.request_add_job(job_id, model, dataset_file_name)
            print(message, end='\n')

        elif input_str[:3] == "rm ":
            cmd, job_id = input_str.split()
            message = communicator.request_remove_job(job_id)
            print(message, end='\n')

        elif input_str[:14] == "job-dashboard ":
            cmd, job_id = input_str.split()
            message = communicator.request_query_dashboard(job_id)
            if type(message) is str:
                print(message, end='\n')
            else:
                job_info, accuracy, query_rate, query_processed_cnt, total_query_count = message # (model, input_dataset_file_name)
                # print
                # todo
                print("===================================== Job Info =====================================", end='\n')
                print("|%-10s|%-20s|%-30s|%-20s|" %("JobID","Status","Input File name","Model"), end='\n')
                print("|%-10s|%-20s|%-30s|%-20s|" %(str(job_id),str(job_info[0]),str(job_info[2]),str(job_info[1])), end='\n')
                print("------------------------------------------------------------------------------------", end='\n')
                print("|%-10s|%-20s|%-30s|%-20s|" %("Accuracy","Query Rate","Query Processed Count","Total Query Count"), end='\n')
                print("|%-10s|%-20s|%-30s|%-20s|" %(accuracy if type(accuracy)==str else str(round(accuracy*100,2)),query_rate if type(query_rate)==str else str(round(query_rate,2)),str(query_processed_cnt),str(total_query_count)), end='\n')
                print("===================================== Progress =====================================", end='\n')
                
                if query_processed_cnt=="?" or total_query_count=="?":
                    continue
                s = "["
                count = int(query_processed_cnt / total_query_count *50)
                for i in range(1,51):
                    if i <= count:
                        s += "="
                    else:
                        s += " "
                s += "] "
                s+= str(round(query_processed_cnt / total_query_count*100,2))
                s +="%"
                print(s, end='\n')
                print()

        elif input_str[:9] == "job-stat ":
            cmd, job_id = input_str.split()
            message = communicator.request_query_stat(job_id)
            if type(message) is str:
                print(message, end='\n')
            else:
                job_info, average, standard_deviation, median, _90th_percentile, _95th_percentile, _99th_percentile = message
                # print
                # todo
                print("===================================== Job Info =====================================", end='\n')
                print("|%-10s|%-20s|%-30s|%-20s|" %("JobID","Status","Input File name","Model"), end='\n')
                print("|%-10s|%-20s|%-30s|%-20s|" %(str(job_id),str(job_info[0]),str(job_info[2]),str(job_info[1])), end='\n')
                print("------------------------------------------------------------------------------------", end='\n')
                print("|%-10s|%-20s|%-30s|%-20s|" %("","Average","Standard Deviation","Median"), end='\n')
                print("|%-10s|%-20s|%-30s|%-20s|" %("",str(average),str(standard_deviation),str(median)), end='\n')
                print("------------------------------------------------------------------------------------", end='\n')
                print("|%-10s|%-20s|%-30s|%-20s|" %("","90th Percentile","95th Percentile","99th Percentile"), end='\n')
                print("|%-10s|%-20s|%-30s|%-20s|" %("",str( _90th_percentile),str( _95th_percentile),str(_99th_percentile)), end='\n')

        elif input_str[:15] == "set-batch-size ":
            cmd, batch_size = input_str.split()
            message = communicator.request_set_batch_size(batch_size)
            print(message, end='\n')
            
        elif input_str[:16] == "show-job-result ":
            cmd, job_id, line_interval_start, line_interval_end = input_str.split()
            message = communicator.request_show_result(job_id, line_interval_start, line_interval_end)
            if type(message) is str:
                print(message, end='\n')
            else:
                job_info, output_file_name, output_file_location_list, data = message # output_file_location_list: list of ip, data: list of string
                # print
                machines_info = []
                for ip in output_file_location_list:
                    machines_info.append(membership.get_node_info(ip))
                print("===================================== Job Info =====================================", end='\n')
                print("|%-10s|%-20s|%-30s|%-20s|" %("JobID","Status","Input File name","Model"), end='\n')
                print("==================================== Output File ===================================", end='\n')
                print("|%-80s|" %(output_file_name), end='\n')
                print("=========================== Machines who holds this file ===========================", end='\n')
                print("|%-10s|%-40s|%-20s|" %("ID","Host Name","IP"), end='\n')
                for host_name, ip, workspace_path, id in machines_info:
                    print("|%-10s|%-40s|%-20s|" %(id,host_name,ip), end='\n')
                print("======================================= Start ======================================", end='\n')
                for i, line in enumerate(data):
                    print(str(i+line_interval_start)+ " "+ line, end="\n")
                print("======================================== END =======================================", end='\n')

        elif input_str == "resource-dashboard":
            message = communicator.request_resource_dashboard()
            if type(message) is str:
                print(message, end='\n')
            else:
                job_infos, job_to_resources = message # job_info = {job: (model, input_dataset_file_name)}, job_to_resources = {job: [ip1, ip2, ip3])}
                # print

                for job_id, resources in job_to_resources.items():
                    job_info = job_infos[job_id]
                    print("===================================== Job Info =====================================", end='\n')
                    print("|%-10s|%-20s|%-30s|%-20s|" %("JobID","Status","Input File name","Model"), end='\n')
                    print("|%-10s|%-20s|%-30s|%-20s|" %(str(job_id),str(job_info[0]),str(job_info[2]),str(job_info[1])), end='\n')
                    print("==================================== Resoueces =====================================", end='\n')
                    for ip in resources:
                        node_info = membership.get_node_info(ip)
                        if node_info == None:
                            continue
                        host_name, ip, workspace_path, id = node_info
                        print("|%-20s|%-40s|%-20s|" %(id,host_name,ip), end='\n')
                    print("=====================================================================================", end='\n')
        elif input_str == "demo-setup":
            SDFS_demo_setup()

        elif input_str[:13] == "update-intro ":
            cmd, ip = input_str.split()
            communicator.update_introducer(ip)
            print("Successfully update introducer as this machine.\n")

        elif input_str == "show-coord":
            node_info = membership.get_node_info(file_controller.master_ip)
            host_name, ip, workspace_path, id = node_info
            print("|%-20s|%-40s|%-20s|" %(id,host_name,ip), end='\n')

        elif input_str == "help":
            print("You may input below command to execute correspond functionalities.\n",
            "Log Querier Commands\n",
            "# grep {option} {pattern}: query log from distributed system.\n",
            "Membership Commands\n",
            "# list_mem: list the membership list.\n",
            "# list_self: list self's id.\n",
            "# join: command to join the group.\n",
            "# leave: command to voluntarily leave the group (different from a failure, which will be Ctrl-C or kill).\n",
            "File System Commands\n",
            "# put {localfilename} {sdfsfilename}: put {localfilename} from local dir as {sdfsfilenamefrom}.\n",
            "# get {sdfsfilename} {localfilename}: fetch {sdfsfilename} to local dir as {localfilename}.\n",
            "# delete {sdfsfilename}\n",
            "# ls {sdfsfilename}: list all machine (VM) addresses where this file is currently being stored.\n",
            "# store: At any machine, list all files currently being stored at this machine.\n",
            "# get-versions {sdfsfilename} {numversions} {localfilename}: gets all the last num-versions versions of the file into the localfilename.\n"
            "Machine Learning Commands\n",
            "# t-start: start training phase\n",
            "# i-start: start inference phase\n",
            "# end: end any phase\n",
            "# add {job id} {model} {training dataset file name}: add training job\n",
            "# add {job id} {model} {input dataset file name}: ad inference job\n",
            "# rm {job id}: remove job\n",
            "# job-dashboard {job id}: display (i) current query rate of a model (job), and (ii) the number of queries processed so far (running count, since the start of the model).\n",
            "# job-stat {job id}: Show current processing time of a query in a given model (job) - show average, standard deviation, median, 90th percentile, 95th percentile, 99th percentile.\n",
            "# set-batch-size {batch size}: Set the “batch size” of queries that are fetched.\n",
            "# show-job-result {job id} {line interval start} {line interval end}: Allow the user to see the results of the queries.\n",
            "# resource-dashboard: Show the current set of VMs assigned to each model/job.\n",
            "Other Commands\n",
            "# help: output command instructions.\n",
            "# exit: forcefully shut down this system\n",
            "# demo-setup\n",
            "# update-intro {ip}\n",
            end=''
            )
        else:
            print("Command Unrecognized.", end='\n')
            print("You can get command instructions by input Command \"help\".", end='\n')

def SDFS_demo_setup():
    """
    preload some file from local dir.
    For demo use.
    """
    # communicator.write_to_SDFS(globals.pwd + "/local_dir/model_Lenet.h5", "model_Lenet.h5")
    # time.sleep(2)
    # communicator.write_to_SDFS(globals.pwd + "/local_dir/model_vgg16.h5", "model_vgg16.h5")
    # time.sleep(2)
    # communicator.write_to_SDFS(globals.pwd + "/local_dir/cifar.csv", "cifar.csv")
    # time.sleep(2)
    # communicator.write_to_SDFS(globals.pwd + "/local_dir/mnist.csv", "mnist.csv")
    # time.sleep(2)
    communicator.request_change_phase(utils.Job_Phase.TRAINING)
    time.sleep(2)
    communicator.request_add_job(10, "LeNet", "")
    time.sleep(2)
    communicator.request_add_job(11, "vgg16", "")
    time.sleep(2)
    communicator.request_change_phase(utils.Job_Phase.NONE)
    print("Demo Setup!")

if __name__ == '__main__':
    # handle argv
    globals.username = sys.argv[1]
    globals.password = sys.argv[2]
    # start time of Server
    timestamp = str(int(time.time()))
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    filename=  (configs.log_path+'host.log'),
                    filemode='w')
    # set up loggers configs
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    gc.collect()
    # initialize modules
    log_querier.init()
    membership.init()
    failure_detector.init()
    communicator.init()
    file_controller.init()
    machine_learning.init()

    # clear files
    file_controller.log_clear()
    file_controller.sdfs_clear()
    
    # create threads
    machine_learning.training("LeNet")
    machine_learning.training("vgg16")
    logging.info('Start create threads\n')
    threads = []
    t_log_querier_server = threading.Thread(target=log_querier.log_querier_server)
    threads.append(t_log_querier_server)

    t_shell = threading.Thread(target=shell)
    threads.append(t_shell)

    t_pinger = threading.Thread(target=failure_detector.pinger)
    threads.append(t_pinger)

    t_monitor = threading.Thread(target=failure_detector.monitor)
    threads.append(t_monitor)

    t_listener = threading.Thread(target=communicator.listener)
    threads.append(t_listener)

    for t in threads:
        t.start()
        #print("1")

    for t in threads:
        t.join()
