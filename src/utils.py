from platform import machine
import socket
class Status:
    ACTIVE = "ACTIVE"
    LEAVE = "LEAVE"
    UNDEFINE = "UNDEFINE"

class Job_Phase:
    TRAINING = "TRAINING"
    INFERENCE = "INFERENCE"
    NONE = "NONE"

class Job_Type:
    TRAINING = "Training"
    INFERENCE = "Inference"

class Job_Status:
    QUEUE = "Queue"
    PROCESSING = "Processing"
    FINISH = "Finish"
    ERROR = "Error"

# Message contruct as [type, sender, ttl, payload]
class MessageType:
    # Membership
    PING = "PING"
    PONG = "PONG"
    JOIN = "JOIN"
    UPDATE = "UPDATE"
    INTRO_UPDATE= "INTRO_UPDATE"
    ELECTION= "ELECTION"
    # client file operation request
    REQ_PUT = "REQ_PUT"
    REQ_GET = "REQ_GET"
    REQ_DELETE = "REQ_DELETE"
    REQ_LIST = "REQ_LIST"
    # master request datanode
    REQ_REPLICATE ="REQ_REPLICATE"
    # master file operation
    WRITE = "WRITE"
    READ = "READ"
    DELETE = "DELETE"
    # REPLY = "REPLY" # reply to client (ACK is enough)
    # client request data node
    GET_DATA = "GET_DATA"
    DATA_WRITE = "DATA_WRITE"
    # data node reply (both master or client)
    ACK = "ACK"
    ERROR_ACK = "ERROR_ACK"
    # job scheduling command by client
    CHANGE_PHASE ="CHANGE_PHASE"
    ADD_JOB = "ADD_JOB"
    REMOVE_JOB = "REMOVE_JOB"
    REQUEST_QUERY_DASHBOARD = "REQUEST_QUERY_DASHBOARD"
    REQUEST_QUERY_STAT = "REQUEST_QUERY_STAT"
    SET_BATCH_SIZE = "SET_BATCH_SIZE"
    REQUEST_JOB_RESULT = "REQUEST_JOB_RESULT"
    REQUEST_RESOURCE_DASHBOARD = "REQUEST_RESOURCE_DASHBOARD"
    # job scheduling command for communicate to client by master
    DISPATCH = "DISPATCH"
    UPDATE_INTRO = "UPDATE_INTRO"
    RECOVER_MASTER = "RECOVER_MASTER"

def get_host_name():
    """
    return local host name
    """
    return socket.gethostname()


def get_host_ip():
    """
    return local ip
    """
    return socket.gethostbyname(get_host_name())
