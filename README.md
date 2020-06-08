## DPT Platform BackEnd (incubating)


#### Work Flow

Read json formatted message from RabbitMQ. Parse the message into a workflow file. 
Submit a spark application to Execute the workflow by spark-submit.  

After the calculation is completed, the spark application will write result to RabbitMQ

#### Instructions

##### Environmental 

- Spark cluster work on Yarn
- RabbitMQ
- pyspark module
- python pika module
- python hdfs module

##### Deploying 

Configure RabbitMQ in Consumer.py file

Use supervisor or similar tools to run a daemon of Consumer.  
for supervisor, conf file like

    [program:dpt_test]
    directory=/home/raoyu/code/DPT/DPT
    command=/home/hadoop/anaconda3/bin/python /home/raoyu/code/DPT/DPT/Consumer.py
    user=raoyu
    autostart=true
    autorestart=true
    redirect_stderr=true
    stdout_logfile=/home/raoyu/code/DPT/DPT/files/log/Consumer.log
    
zip lib dir into lib.zip

Write json formatted message to RabbitMQ

##### example

Template json file is in files/example.json

Example data set is in files/sample_libsvm_data.txt

To test the project

    Change pika args of RabbitMQ in Consumer.py, Producer.py

    Run Producer.py in lib.utils. It will write example.json to RabbitMQ
