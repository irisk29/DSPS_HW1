# DSPS_HW1
In this assignment you will code a real-world application to distributively process a list of PDF files, perform some operations on them, and display the result on a web page.
## How to run our project
1. Copy and paste your credentials into ~/.aws/credentials.
2. Create a bucket in s3.
3. Create `Manager.jar` and `Worker.jar` and upload them inside the bucket.
4. Create a file `LocalApplication/secure_info.json` -
  The content of the file should be
    ```{
      "ami" : "<image to run for manager and workers>",
      "arn" : "<roles for the ec2 instances>",
      "keyName" : "<name of the keyPair>",
      "securityGroupId" : "<security group id>",
      "jarsBucketName" : "<name of the bucket you created>"
    }
   ```
5. run local app with 4 args:
   1. `args[0]`: input file name path
   2. `args[1]`: output file name
   3. `args[2]`: workers’ files ratio 
   4. `args[3]`: (optional) must be equals to "terminate" in order to terminate the application at the end
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
17. Local application sends a terminate message to the manager if it received terminate as one of its arguments.
18. If Manager gets termination message, he deletes "tasksqueue".
19. The Workers see that the "tasksqueue" not exists anymore and they get terminate.
20. Manager clean the resources and gets terminate.
## Technical stuff
- We used Ubuntu 20.04 based AMI, and instance type of T2-micro
- when used with 10 Workers and 24 links it takes about 3 minutes (including manager and workers startup time)

## Considerations
### Security
All our sensitive data and credentials saved on files in our local computer. When we created a new EC2 client we gave the permissions needed for him and therefore there was no need to pass the credentials. For the sensitive data, we transferred it in the User Data and did not write them in plain text.
### Scalability?
Yes, on one hand, the Manager doesn't hold local app inforation on the ram, only the amount of "connected" local apps.
- The personal data such as number of urls remaining and return bucket name are held in temp bucket, its name is the personal return SQS queue of the local app.
- The Manager app reads the links file that the local app uploaded for him line after line **dynamically** such that it won't be resource consuming to read the hole file.
- The local app reads the final <link, OCR output> entries from the bucket entry **dynamically** when creating the file.
- Worker App won't save the image on disk, and reads it to memory **dynamically** (we assume that the image size won't pass 800 MB)
- We assume that image OCR description won't pass 200 KB
### Persistence?
- We're catching all possible exeptions that might rais from the operation of our code, we do not protect against sudden termination from Amazon itself.
### Threads in our application
Only the Manager uses more then one thread:
- `proccess wokers finished tasks thread`: responsible for receiving finished messages from workers and append them to the relevant summery file. once summery file is full of all operations, it sends it to the local application. 
- `waiting for new local application task thread`: waits for new task. for each one it gets, start new thread from the thread poll and waits for new message.
- `handle new local application task thread`: we manage thread poll for threads that can handle new task - send message for each URL in the file and create the number of needed workers.
