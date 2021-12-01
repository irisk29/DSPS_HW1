# DSPS_HW1
In this assignment you will code a real-world application to distributively process a list of PDF files, perform some operations on them, and display the result on a web page.
## How to run our project
1. Copy and paste your credentials into ~/.aws/credentials.
2. Create a file `LocalApplication/secure_info.json` -
  The content of the file should be
    ```{
      "ami" : "<image to run for manager and workers>",
      "arn" : "<roles for the ec2 instances>",
      "keyName" : "<name of the keyPair>",
      "securityGroupId" : "<security group id>",
    }
   ```
3. Create a bucket in s3 named `workermanagerjarsbucket`.
4. Create `Manager.jar` and `Worker.jar` and upload them inside the bucket.
5. run local app with 4 args:
   1. `args[0]`: input file name path
   2. `args[1]`: output file name
   3. `args[2]`: workers’ files ratio 
   4. `args[3]`: (optional) must be equals to "terminate" in order to terminate the application at the end
## Application Flow
1. Local Application uploads the file with the list of PDF files and operations to S3.
2. Local Application sends a message (queue) stating the location of the input file on S3.
3. Local Application does one of the two:
  ▪ Starts the manager.
  ▪ Checks if a manager is active and if not, starts it.
4. Manager downloads list of PDF files together with the operations.
5. Manager creates an SQS message for each URL and operation from the input list.
6. Manager bootstraps nodes to process messages.
7. Worker gets a message from an SQS queue.
8. Worker downloads the PDF file indicated in the message.
9. Worker performs the requested operation on the PDF file, and uploads the resulting output 
to S3.
10. Worker puts a message in an SQS queue indicating the original URL of the PDF file and the S3 
URL of the output file, together with the operation that produced it.
11. Manager reads all Workers' messages from SQS and creates one summary file, once all URLs 
in the input file have been processed.
12. Manager uploads the summary file to S3.
13. Manager posts an SQS message about the summary file.
14. Local Application reads SQS message.
15. Local Application downloads the summary file from S3.
16. Local Application creates html output file.
17. Local application send a terminate message to the manager if it received terminate as one of 
its arguments.

## Considerations
### Did you think about scalability?
Yes, on one hand, the Manager doesn't hold local app inforation on the ram, only the amount of "connected" local apps.
- The personal data such as number of urls remaining and return bucket name are held in temp bucket, its name is the personal return SQS queue of the local app.
- The Manager app reads the links file that the local app uploaded for him line after line **dynamically** such that it won't be resource consuming to read the hole file.
- The local app reads the final <link, OCR output> entries from the bucket entry **dynamically** when creating the file.
- Worker App won't save the image on disk, and reads it to memory **dynamically** (we assume that the image size won't pass 800 MB)
- We assume that image OCR description won't pass 200 KB
### What about persistence?
- We're catching all possible exeptions that might rais from the operation of our code, we do not protect against sudden termination from Amazon itself.
### Threads in our application
Only the Manager uses 2 thread:
- thread `main`: responsible for receiving messages (up to 10 at a time) from local apps and sending tasks to the workers
- thread `WorkerToManager`: responsible for receiving messages (up to 10 at a time) and upload their cotent to the proper S3 bucket of the relevant local app
## Technical stuff
- We used Ubuntu 20.04 based AMI, and instance type of T2-micro
- when used with 10 Workers and 24 links it takes about 3 minutes (including manager and workers startup time)
