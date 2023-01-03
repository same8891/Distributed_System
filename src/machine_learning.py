import communicator
import utils
import globals
import file_controller
import configs

import time
import numpy as np
import tensorflow as tf
from tensorflow.keras.datasets import cifar10
from tensorflow.keras import Model
from tensorflow.keras.models import Sequential
from tensorflow.keras.losses import categorical_crossentropy
from tensorflow.keras.layers import Dense, Flatten, Conv2D, AveragePooling2D
from tensorflow.keras.optimizers import SGD
from tensorflow.keras import datasets
from tensorflow.keras.utils import to_categorical

def init():
    global models
    models = {}

def training(model):
    global models
    # fetch file
    if model=="LeNet":
        # communicator.read_from_SDFS(globals.pwd + "/sdfs_dir/model_Lenet.h5", "model_Lenet.h5", 1)
        # communicator.read_from_SDFS(globals.pwd + "/sdfs_dir/mnist.csv", "mnist.csv", 1)
        time.sleep(10)
        models[model] = img_model1()
        print("Loaded LeNet")
    elif model == "vgg16":
        # communicator.read_from_SDFS(globals.pwd + "/sdfs_dir/model_vgg16.h5", "model_vgg16.h5", 1)
        # communicator.read_from_SDFS(globals.pwd + "/sdfs_dir/cifar.csv", "cifar.csv", 1)
        time.sleep(20)
        models[model] = img_model1()  
        print("Loaded VGG16")
        

def get_query(model, query_id, cnt, batch_size):
    if model=="LeNet":
        f = open(globals.pwd + "/local_dir/mnist.csv", "r") 
    elif model == "vgg16":
        f = open(globals.pwd + "/local_dir/cifar.csv", "r") 
    
    # skip first line (schema)
    next(f)
    # skip processed lines if some queries processed.
    for i in range(batch_size*query_id):
            next(f)
    query = []

    for line in f:
        if len(query)==cnt:
            break
        query.append(line)
    f.close()
    return query


def inference(job_id, model, query_id, cnt, batch_size):
    global models
    if model=="LeNet":
        result = inference1(get_query(model, query_id, cnt, batch_size), models[model])
    elif model == "vgg16":
        result = inference1(get_query(model, query_id, cnt, batch_size), models[model])
    # print("after inference", result)
    ack_msg = [utils.MessageType.ACK, utils.get_host_ip(), 1, (job_id, model, query_id, result)]
    # print("before send in ML", ack_msg)
    communicator.udp_send(ack_msg, file_controller.master_ip, configs.query_reply_receiver_port)
    
def img_model1():
    class LeNet(Sequential):
        def __init__(self,**kwargs):
            super().__init__()

            self.add(Conv2D(6, kernel_size=(5, 5), strides=(1, 1), activation='tanh', input_shape=(28,28,1), padding="same"))
            self.add(AveragePooling2D(pool_size=(2, 2), strides=(2, 2), padding='valid'))
            self.add(Conv2D(16, kernel_size=(5, 5), strides=(1, 1), activation='tanh', padding='valid'))
            self.add(AveragePooling2D(pool_size=(2, 2), strides=(2, 2), padding='valid'))
            self.add(Flatten())
            self.add(Dense(120, activation='tanh'))
            self.add(Dense(84, activation='tanh'))
            self.add(Dense(10, activation='softmax'))
            self.compile(optimizer='adam',loss=categorical_crossentropy,metrics=['accuracy'])
    model = LeNet()
    while True:
        try:
            model.load_weights(globals.pwd + "/local_dir/model_Lenet.h5")
            break
        except Exception:
            print("error")
    return model


def inference1(input,model):
    start = time.time()
    tmp = []
    for i in input:
        s = i.split(',')
        tmp.append(s)
    data_all = np.array(tmp)
    labels = data_all[:,0]
    labels = labels.astype('int8')
    data = data_all[:,1:]
    data = data.reshape((data.shape[0],28,28))
    data = data[:, :, :, np.newaxis]
    data = data.astype('float32')
    data /= 255
    # print(data)
    while True:
        try:
            prediction_values = model.predict(data)
            break
        except Exception:
            model = img_model1()
            models["LeNet"] = model
    prediction_values = np.argmax(prediction_values,axis=1)
    prediction_values = prediction_values.astype('int8')
    sum = 0
    for i in range(len(prediction_values)):
        if prediction_values[i] == labels[i]:
            sum = sum + 1
    acc = sum/len(prediction_values)
    end =  time.time()
    return (len(prediction_values), int(sum), end-start, list(map(str,prediction_values)))



def img_model2():
    class LeNet(Sequential):
        def __init__(self,**kwargs):
            super().__init__()

            self.add(Conv2D(6, kernel_size=(5, 5), strides=(1, 1), activation='tanh', input_shape=(28,28,1), padding="same"))
            self.add(AveragePooling2D(pool_size=(2, 2), strides=(2, 2), padding='valid'))
            self.add(Conv2D(16, kernel_size=(5, 5), strides=(1, 1), activation='tanh', padding='valid'))
            self.add(AveragePooling2D(pool_size=(2, 2), strides=(2, 2), padding='valid'))
            self.add(Flatten())
            self.add(Dense(120, activation='tanh'))
            self.add(Dense(84, activation='tanh'))
            self.add(Dense(10, activation='softmax'))
            self.compile(optimizer='adam',loss=categorical_crossentropy,metrics=['accuracy'])
    model = LeNet()
    while True:
        try:
            model.load_weights(globals.pwd + "/local_dir/model_vgg16.h5")
            break
        except Exception:
            pass
    return model


def inference2(input,model):
    start = time.time()
    tmp = []
    for i in input:
        s = i.split(',')
        tmp.append(s)
    data_all = np.array(tmp)
    labels = data_all[:,3072]
    labels = labels.astype('int8')
    data = data_all[:,:3072]
    data = data.reshape((data.shape[0],32,32,3))
    data = data[:, :, :, np.newaxis]
    data = data.astype('float32')
    data /= 255
    # print(data)
    while True:
        try:
            prediction_values = model.predict(data)
            break
        except Exception:
            model = img_model2()
            models["vgg16"] = model
    prediction_values = np.argmax(prediction_values,axis=1)
    # print(prediction_values)
    prediction_values = prediction_values.astype('int8')
    sum = 0
    for i in range(len(prediction_values)):
        if prediction_values[i] == labels[i]:
            sum = sum + 1
    end =  time.time()
    return (len(prediction_values), int(sum), end-start, list(map(str,prediction_values)))
