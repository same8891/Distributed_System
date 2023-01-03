import threading
import utils
import json
import configs
import os

class node:
    def __init__(self, host_name="", ip="", workspace_path="", id=-1) -> None:
        self.host_name = host_name
        self.ip = ip
        self.id = id
        self.workspace_path = workspace_path
        # self.status = status
        self.next = None
        self.prev = None

def init():
    global lock, ip_to_node, id_to_node, count
    lock = threading.Lock()
    ip_to_node = {}
    id_to_node = {}
    count = 0


def clear():
    """
    initialize membership
    """
    global lock, ip_to_node, id_to_node, count
    lock.acquire()
    
    for node in ip_to_node.values():
        del node
    ip_to_node.clear()
    id_to_node.clear()
    count = 0

    lock.release()
    return

def id_allocate():
    """
    No input
    implement id allocate algorithm in here
    Return unique positive integer id
    """
    global lock, ip_to_node, id_to_node, count
    lock.acquire()
    
    if len(ip_to_node)!=0:
        id = max(id_to_node.keys())+1
    else:
        id = 1

    lock.release()
    return id

def is_member(ip):
    """
    input ip
    return True or False that whether if this ip already in membership.
    """
    global lock, ip_to_node, id_to_node, count
    lock.acquire()
    
    val = (ip in ip_to_node)

    lock.release()
    return val

def insert_node(host_name, ip, workspace_path, id=-1):
    """
    input host_name, ip, id(optinal)
    if don't input id, id would be automatic generated.
    add new member into membership list
    return None
    """
    global lock, ip_to_node, id_to_node, count
    if id==-1:
        id = id_allocate()

    lock.acquire()

    new_node = node(host_name, ip, workspace_path, id)
    if ip in ip_to_node:
        pass
    elif count==0:
        new_node.next = new_node
        new_node.prev = new_node
        ip_to_node[ip] = new_node
        id_to_node[id] = new_node
        count+=1
    elif count==1:
        # in this condition, only introducer need to insert new node
        cur = ip_to_node[utils.get_host_ip()]
        new_node.next = cur
        new_node.prev = cur
        cur.next = new_node
        cur.prev = new_node
        ip_to_node[ip] = new_node
        id_to_node[id] = new_node
        count+=1            
    else:
        cur = ip_to_node[utils.get_host_ip()]
        while not ((cur.id < id and id < cur.next.id) or (cur.id < id and cur.id > cur.next.id)):
            cur = cur.next
        new_node.next = cur.next
        new_node.prev = cur
        cur.next.prev = new_node
        cur.next = new_node
        ip_to_node[ip] = new_node
        id_to_node[id] = new_node
        count+=1  

    lock.release()
    return

def delete_node(ip):
    """
    input ip
    delete this member
    return None
    """
    global lock, ip_to_node, id_to_node, count
    lock.acquire()

    if ip not in ip_to_node:
        lock.release()
        return None

    cur = ip_to_node[ip]
    tnext = cur.next
    tprev = cur.prev
    tnext.prev = tprev
    tprev.next = tnext
    del ip_to_node[cur.ip]
    del id_to_node[cur.id]
    del cur
    count -= 1

    lock.release()
    return

def get_next_member_info(ip):
    """
    input ip
    return next member's info format (host_name, ip, workspace_path, id)
    if no such member, return None
    """
    global lock, ip_to_node, id_to_node, count
    lock.acquire()
    if count <= 1:
        lock.release()
        return None
    # key not exist
    if ip not in ip_to_node:
        lock.release()
        return None
    node = ip_to_node[ip].next
    info = (node.host_name, node.ip, node.workspace_path, node.id)

    lock.release()

    return info

def get_prev_member_info(ip):
    """
    input ip
    return prev member's info format (host_name, ip, workspace_path, id)
    if no such member, return None
    """
    global lock, ip_to_node, id_to_node, count
    lock.acquire()
    if count <= 1:
        lock.release()
        return None
    # key not exist
    if ip not in ip_to_node:
        lock.release()
        return None
    node = ip_to_node[ip].prev
    info = (node.host_name, node.ip, node.workspace_path, node.id)

    lock.release()

    return info

def membership_construct(membership_message):
    """
    construct membership by input list (received from introducer)
    membership_message format is a list of tuples (host_name, ip, workspace_path, id)
    """
    # process order of membership_message
    # todo
    global lock, ip_to_node, id_to_node, count
    clear()

    lock.acquire()

    cur = None
    for host_name, ip, workspace_path, id in membership_message:
        if count==0:
            new_node = node(host_name, ip, workspace_path, id)
            new_node.next = new_node
            new_node.prev = new_node
            ip_to_node[ip] = new_node
            id_to_node[id] = new_node
            cur = new_node
            count+=1
        elif count==1:
            new_node = node(host_name, ip, workspace_path, id)
            new_node.next = cur
            new_node.prev = cur
            cur.next = new_node
            cur.prev = new_node
            ip_to_node[ip] = new_node
            id_to_node[id] = new_node
            cur = new_node
            count+=1
        else:
            new_node = node(host_name, ip, workspace_path, id)
            new_node.next = cur.next
            new_node.prev = cur
            cur.next.prev = new_node
            cur.next = new_node
            ip_to_node[ip] = new_node
            id_to_node[id] = new_node
            cur = new_node
            count+=1
    
    lock.release()
    return

def get_membership_message():
    """
    construct membership_message that send to other processes from this data structure
    membership_message format is a list of tuples (host_name, ip, workspace_path, id)
    """        
    global lock, ip_to_node, id_to_node, count
    lock.acquire()

    membership_message = []
    for id, node in id_to_node.items():
        membership_message.append((node.host_name, node.ip, node.workspace_path, node.id))
    lock.release()

    return membership_message

def get_node_info(ip):
    """
    input certain ip
    return its correspond (host_name, ip, workspace_path, id)
    """
    global lock, ip_to_node, id_to_node, count
    lock.acquire()
    # key not exist
    if ip not in ip_to_node:
        lock.release()
        return None
    node = ip_to_node[ip]
    info = (node.host_name, node.ip, node.workspace_path, node.id)

    lock.release()

    return info

def get_membership_number():
    """
    return alive member count in membership
    """
    global count
    return count

def store_membership():
    """
    no input & output
    store memberhsip in configs.log_path + "membership.json"
    """
    data = json.dumps(get_membership_message())
    jsonFile = open(configs.log_path + "membership.json", "w")
    jsonFile.write(data)
    jsonFile.close()
    return

def restore_membership():
    """
    no input
    restore memberhsip from configs.log_path + "membership.json"
    if no such file reutrn False
    else return True
    """
    if os.path.getsize(configs.log_path + "membership.json")==0:
        return False
    jsonFile = open(configs.log_path + "membership.json", "r")
    data = jsonFile.read()
    jsonFile.close()
    membership_message = json.loads(data)
    membership_construct(membership_message)
    return True

######################
# For debug purposes #
######################
def print_node_topology():
    """
    print linked list ring topology 
    """
    global lock, ip_to_node, id_to_node, count
    lock.acquire()
    print("================================================\n")
    print("=========== Topology Membership List ===========\n")
    print("|%20s|%20s|%20s|\n" %("ID") %("Host Name") %("IP"))
    cur = ip_to_node[utils.get_host_ip()]
    print("|%20s|%20s|%20s|\n" %(cur.id) %(cur.host_name) %(cur.ip))
    cur=cur.next
    while cur.ip != utils.get_host_ip():
        print("|%20s|%20s|%20s|\n" %(cur.id) %(cur.host_name) %(cur.ip))
        cur=cur.next   
    lock.release()        





