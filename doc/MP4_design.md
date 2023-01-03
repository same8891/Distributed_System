training phase
* t-start
* add {job id} {model} {training dataset file name} 
* rm {job id}
* end
inference phase
* i-start
* add {job id} {model} {input dataset file name}
* rm {job id}
* end
display
* (C1) job-dashboard: (i) current query rate of a model (job), measured over the last 10 seconds (i.e., a moving window of last 10 seconds), and (ii) the number of queries processed so far (running count, since the start of the model).
* (C2) job-stat: Show current processing time of a query in a given model (job) - please show average, percentiles, standard deviation, etc. (see MP spec). 
* (C3) set-batch-size {batch size}: Set the “batch size” of queries that are fetched (this can be used to implicitly change the query rate).
* (C4) show-job-result {job id} {line interval start} {line interval end}: Allow the user to see the results of the queries, and that queries are actually being processed (this might involve commands to open and view the file where results are being stored, etc.)
* (C4) ?: need more command to support (C4)
* (C5) resource-dashboard: Show the current set of VMs assigned to each model/job.
 
1. [-] modify MP3 (ftp), refactor MP3 backbone, required!!! 
    * membership store member workspace path
    * file transfer use ftp replace tcp 
2. [ ] MP1 debugging
3. [ ] job scheduling (MP4)
    * handle by master thread
    * phase status maintain by master
    * job reallocation thread
    * metadata contains:
        1. model_list: List
        2. model_to_nodes: dict{str:List}
        3. model_to_jobs: dict{str:List}
    * display progress in dashboard
        * when finish any batch, send a message. 
        * track progress by master

4. [ ] ML module
    * training()
    * inferece_for_image_classification()
    * inferece_for_text_classification()
