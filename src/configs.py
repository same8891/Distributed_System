# paths
log_path = "./log_dir/"
sdfs_path = "./sdfs_dir/"
local_path = "./local_dir/"

# socket ports
# for listener
listener_port = 6889
# for log_querier
log_querier_port = 6890
# for master
master_port = 6891
master_ack_receiver_port = 8000 # reserve 8000 - 8010
master_timeout=1
# for client requests
request_ack_port = 7050 # 7050-7060
# for datanode
file_transfer_port = 7000 # 7000-7010
query_reply_receiver_port = 5000


# file system configs
replica_num = 4
read_replica_num = replica_num/2
write_replica_num = replica_num/2+1
ack_timeout = 30
re_replica_period = 10

# failure detection configs
ping_period = 0.5
monitor_period = 3
fail_timeout = 9
piggyback_probablity = 1000000000000 # higher is low probability
store_membership_period = 20
store_metadata_period = 20
auto_store_master = 25

# job shceduling configs
job_scheduler_period = 10
load_balance_threshold = 0.07
dispatch_speed =0.1

# machine_list
machine_list = [
    "172.22.156.52",
    "172.22.158.52",
    "172.22.94.52",
    "172.22.156.53",
    "172.22.158.53",
    "172.22.94.53",
    "172.22.156.54",
    "172.22.158.54",
    "172.22.94.54",
    "172.22.156.55"
]
