# DSPS_HW1
In this assignment you will code a real-world application to distributively process a list of PDF files, perform some operations on them, and display the result on a web page.
## How to run our project
1. Copy and paste your credentials into ~/.aws/credentials.
2. Create a bucket in s3.
3. Create `Manager.jar` and `Worker.jar` and upload them inside the bucket.
4. Create a file `LocalApplication/secure_info.json` -
  The content of the file should be
    ```
    {
      "ami" : "<image to run for manager and workers>",
      "arn" : "<roles for the ec2 instances - must contain full access to S3, SQS and EC2>",
      "keyName" : "<name of the keyPair>",
      "securityGroupId" : "<security group id>",
      "jarsBucketName" : "<name of the bucket you created - must be an unique name in us-east-1 region>"
    }
   ```
5. run local app with 4 args:
   1. `args[0]`: input file name path
   2. `args[1]`: output file name
   3. `args[2]`: workers’ files ratio 
   4. `args[3]`: (optional) if not empty, terminate the application at the end
## Application Flow
1. Local Application uploads the file with the list of PDF files and operations to S3.
2. All Local Applications sends a message in the same queue named "lamqueue" stating the location of their input file on S3 and the queue name where they expect to get the finished message (unique queue per Local Application - "malqueue" + localID).
3. Manager waits for messages in the "lamqueue".
4. Once he gets one, for each URL in the input list he sends message in the "tasksqueue".
5. Manager bootstraps workers nodes to process messages.
6. Worker waits for messages in the "tasksqueue".
7. Once he gets one, he downloads the PDF file indicated in the message.
8. Worker performs the requested operation on the PDF file, and uploads the resulting output to S3.
9. Worker puts a message in the "finishedtasksqueue" indicating the S3 URL of the output file.
10. Manager waits for finished messages in the "finishedtasksqueue".
11. For each finished message he gets, he adds it to the Local Application's summary file.
12. Manager uploads the summary file to S3.
13. For each Local Application's summary file, Manager sends an SQS message in the "malqueue" + localID.
14. Local Application reads final message from the "malqueue" + localID.
15. Local Application downloads the summary file from S3.
16. Local Application creates html output file.

![dspshw1flow drawio](https://user-images.githubusercontent.com/48298162/144744422-c58abe04-9201-4869-bd95-36cbdbaede14.png)

## Termination Flow
1. Local application sends a terminate message to the manager if it received terminate as one of its arguments.
2. If Manager gets termination message, he deletes "lamqueue" for not getting more tasks from local applications.
3. Stops all threads that sends new messages to "tasksqueue".
4. Waits until all tasks in the "tasksqueue" were handeled, and then delete this queue.
5. The Workers see that the "tasksqueue" not exists anymore and they stop themself.
6. Manager terminates all stopped workers.
7. Manager waits until all finished tasks in the "finishedtasksqueue" were handeled, and then delete this queue.
8. The Manager gets terminate.

## Technical stuff
- We used Ubuntu 20.04 based AMI, and instance type of T2-micro for the workers and T2-Medium for the manager.
- When we run the input-sample-1.txt which contains 2500 links with n=250 it took about 15 minutes for the all process to run.
- When we run the input-sample-2.txt which contains 100 links with n=10 it took about 2 minutes for the all process to run.

## Considerations
### Security
The aws credentials are not presented in plain text. We created a credentials file inside the ~/.aws folder from which we derive the credentials that are needed to perform the necessary actions. Other private data that we didn't want to be expose to anyone (ami, arn, securityGroupId, ect.), we read from json file which exists only on our local device.
### Scalability?
We considered the scalability matter by the following aspects:
1. Thread pool - we did not use a TPS (Thread Per Client) methodology because we cannot assign TPS when we have a large amount of client, for example 1 billion clients. Using the thread pool we assigned the maximum number of threads we can in order to process the local application requests simultaneously.
2. Increasing the number of workers (if needed) - when we receive a job from a local application and n, we calculate the number of needed workers and create them accordingly. This way we scale-up when only it is needed (of course we took in considereation the limitation of 8 ec2 instances we have but potentially our program can increase to an higher numer to support 1 billion of clients for example).
3. We used linear amount of SQS - we created N SQS when N is the number of local application. This way we ensure we do not use too much resources than necessary.
4. We used two S3 buckets - we tried to ensure we do not use too much resources than necessary.
5. We used linear amount of memory in the manager - we downloaded only the input files of the local applications but not the final files which are used in the summary file that will be sent to an appropriate local application. This way we tried to ensure that the memory of the manager won't run out easily.
### Persistence?
- We're catching all possible exeptions that might raise from the operation of our code.
- Visibility Timeout: using this mechanism, each message that is hanled by the manager or the workers have limited time in which it is assigned to appropriate ec2 instance. During this time if the ec2 instance that is handling the message, terminates suddenly or stalls, when the visibility timeout is reached the message is back to be visible again to all the instances. This way other worker can handle the task.
- FIFO SQS + Message Duplication ID: this way we can ensure there are no duplications. So if the commincation broke and a message sent twice we will discard the duplication.
### Threads in our application
Only the Manager uses more then one thread:
- `proccess workers finished tasks thread`: responsible for receiving finished tasks from the workers and append them to the relevant summery file. Once a summery file is full with all the results, it sends it to the appropriate local application's SQS. 
- `waiting for new local application job thread`: This is the main thread of the manager. It waits for new local application job. A job is an .txt file which every line contains an operation to perform over a link. For each one it gets, it downloads the file locally and passes it to the thread pool, there a thread will be assigned to handle this job. After that, it returns to the beginning and waits for new jobs from the local applications.
When a termination message is received it acivates the termination flow as described in that section.
- `handle new local application job thread`: Each thread in the thread pool receive a job which is a file as described above. First it checks if we need more workers than we currently have to proccess the new job. If we do, it creates the necessary amount of workers. After that, it iterates over the file, and from each line it creates a new task for the workers to handle.
### Limitation
- Number of EC2 clients: we ensure that we never creates more than <aws limitation number> EC2 clients.
- 100$ in the account: we mostly used the free tier resources to keep up with the budget and used the appropriate resources that can support the task we gave and did not choose randomly with financial considerations.
