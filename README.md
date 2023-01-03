# cs425-mp4


# IDunno, a Distributed Learning Cluster


## Getting started
This project implement a Distributed Learning Cluster

## How to setup a new machine?
First, set up SSH by [Use SSH keys to communicate with GitLab ](https://docs.gitlab.com/ee/user/ssh.html).

1. Clone Repo
```
cd
git clone git@gitlab.engr.illinois.edu:whhuang4/cs425-mp4.git
```

2. Setup Enviroment
```
cd
sudo yum install python3.8
python3 -m pip install --user --upgrade pip
pip install --upgrade tensorflow
pip install scp
pip install paramiko
pip install numpy
```


## Command of Simple Distributed File System
Start
```
cd cs425-mp4
python3 src/main.py {username} {password}
{command}
```
* Log Querier Commands
    * grep {option} {pattern}
        * Examples:
        * grep -c {Pattern}
        * grep -Ec {Pattern}
* Membership Commands
    * list_mem: list the membership list
    * list_self: list self’s id
    * join: command to join the group
    * leave: command to voluntarily leave the group (different from a failure, which will be Ctrl-C or kill)
* File System Commands
    * put {localfilename} {sdfsfilename}: put {localfilename} from local dir as {sdfsfilenamefrom}
    * get {sdfsfilename} {localfilename}: fetch {sdfsfilename} to local dir as {localfilename}
    * delete {sdfsfilename}
    * ls {sdfsfilename}: list all machine (VM) addresses where this file is currently being stored
    * store: At any machine, list all files currently being stored at this machine
    * get-versions {sdfsfilename} {numversions} {localfilename}: gets all the last num-versions versions of the file into the localfilename (use delimiters to mark out versions)
* Machine Learning Commands
    * t-start: start training phase
    * i-start: start inference phase
    * end: end any phase
    * add {job id} {model} {training dataset file name}: add training job
    * add {job id} {model} {input dataset file name}: ad inference job
    * rm {job id}: remove job
    * job-dashboard {job id}: display (i) current query rate of a model (job), and (ii) the number of queries processed so far (running count, since the start of the model).
    * job-stat {job id}: Show current processing time of a query in a given model (job) - show average, standard deviation, median, 90th percentile, 95th percentile, 99th percentile.
    * set-batch-size {batch size}: Set the “batch size” of queries that are fetched.
    * show-job-result {job id} {line interval start} {line interval end}: Allow the user to see the results of the queries.
    * resource-dashboard: Show the current set of VMs assigned to each model/job.
* Other
    * demo-setup
    * update-intro {ip}
## Contributors
whhuang4: whhuang4@illinois.edu 

zyl2: zyl2@illinois.edu
