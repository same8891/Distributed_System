### Functionality

1. command shell
2. distributed log query
3. distributed membership management
    [-] id allocate
    [-] failure detection
4. distributed file system
    [-] master election: Ring election
    [-] file management in local
    [-] file management in master (under file_controller.py) (master only return one address of replicas)
    [-] metadata management by master (under file_controller.py)
    [-] file allocation: average_allocate/Chord (under file_controller.py)
    [ ] mutaul exclusion: bully algorthm (optional)
    [-] Read & Write Mechanism (under file_controller.py)
    [-] Re-replicas
        * https://data-flair.training/blogs/hadoop-hdfs-data-read-and-write-operations/#:~:text=HDFS%20follow%20Write%20once%20Read,into%2Ffrom%20the%20respective%20datanodes.
5. ML
    [ ] job scheduling
    [ ] training
    [ ] inference

### System

1. Ring backbone
2. Versioned file system
3. Total ordering: Sequencer-based Approach
4. Consistency level: reads by R replicas and writes by W replicas

### Configs

* N = 4 (replica number)
* W = N/2+1 = 3
* R = N/2 = 2

# my note
1. 把 input file 丟到 master
2. 讀檔案 然後切開處理後丟到 我分配的機器上 然後根據反饋 我可以動態調整要丟給誰
3. 用id檢查順序 append到一個file中
4. 最後丟進sdfs

[-] design two function to modify SDFS infra.
write_to_SDFS(file_path, sdfs_file_name) # put, (write output file to SDFS)
read_from_SDFS(file_path, sdfs_file_name, version) # get, get-versions,  (read file to master)

考量到coordinator down
要有recovery機制
定期寫東西到SDFS然後recovery時讀出來
這類 metadata or input or output 在臨時狀態時 加上 .working extension
