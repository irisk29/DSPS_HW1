import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.IamException;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    static boolean gotTerminate = false;
    final static int MAX_T = 5;
    static ConcurrentHashMap<String, Integer> map
            = new ConcurrentHashMap<>();

    public static boolean gotTerminationMsg(Message message)
    {
        return message.body().equals("Terminate");
    }

    public static String createQueue(SqsClient sqsClient, String queueNameToCreate)
    {
        try {
            String queueName = queueNameToCreate + System.currentTimeMillis() + ".fifo"; //msg only sent once
            Map<String, String> attributes = new HashMap<String, String>();
            attributes.put("FifoQueue", "true");
            attributes.put("ContentBasedDeduplication", "true");
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributesWithStrings(attributes)
                    .build();

            sqsClient.createQueue(createQueueRequest);

            GetQueueUrlResponse getQueueUrlResponse =
                    sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
            String queueUrl = getQueueUrlResponse.queueUrl();
            System.out.println(queueNameToCreate + " queue url: " + queueUrl);
            return queueUrl;
        }catch (Exception exception)
        {
            System.out.println(exception.getMessage());
        }
        return "";
    }

    public static int getFileSize(File file)
    {
        int fileSize = 0;
        try {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if(line.contains("§")) //every finished task will end with this symbol
                        fileSize++;
                }
            }
        }catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
        return fileSize;
    }

    public static void SendMessage(SqsClient sqsClient, String queueUrl , String msgBody)
    {
        try {
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageGroupId("finishedtask")
                    .messageBody(msgBody)
                    .build());

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    /*message from worker will be in the format - <original_pdf_url, the S3 url of the new
    image file, operation that was performed, finalMsgQueueName>
    where finalMsgQueueName represents the queue name between local application and the manager*/
    public static void processFinishedTasks(S3Client s3, SqsClient sqsClient, String finishedTasksQueueURL)
    {
        System.out.println("In start of processFinishedTasks");
        S3Methods s3Methods = S3Methods.getInstance();
        SQSMethods sqsMethods = SQSMethods.getInstance();

        while(!gotTerminate)
        {
            Message finishedTask = sqsMethods.receiveMessage(finishedTasksQueueURL); //reads only 1 msg
            System.out.println("got finished task");
            String[] msgBody = finishedTask.body().split("\\$");
            System.out.println("msg body is: " + Arrays.toString(msgBody));
            String result = "", finalMsgQueueName = "";
            if(msgBody[0].equals("taskfailed")) {
                // action + "$" + pdfStringUrl + "$" + localAppID + "$" + resultFilePath.getValue();
                String action = msgBody[1];
                String pdfStringUrl = msgBody[2];
                finalMsgQueueName = msgBody[3];
                String exceptionDesc = msgBody[4];
                result = action + "$" + pdfStringUrl + "$" + exceptionDesc + "§";
            }
            else {
                String original_pdf_url = msgBody[0];
                String bucketName = msgBody[1];
                String keyName = msgBody[2];
                String operation = msgBody[3];
                finalMsgQueueName = msgBody[4];
                result = original_pdf_url + "$" + bucketName + "$" + keyName + "$" + operation + "§";
            }

            System.out.println("result: " + result + " finalMsgQueueName: " + finalMsgQueueName);

            int fileSize = map.get(finalMsgQueueName);
            try
            {
                File f = new File(finalMsgQueueName + ".txt");
                PrintWriter out = new PrintWriter(new FileWriter(f, true));
                out.println(result);
                out.close();
                int currFileSize = getFileSize(f);
                if(currFileSize == fileSize) //finished all the tasks for certain local application
                {
                    String bucket_name = "finishedtasksbucket";
                    if(!s3Methods.isBucketExist(bucket_name))
                        s3Methods.createBucket(bucket_name);
                    String key_name = finalMsgQueueName + System.currentTimeMillis();
                    s3Methods.putObject(bucket_name, key_name, f); //uploading the summary file to s3

                    String doneMsg = bucket_name + "$" + key_name; //location of the summary file in s3
                    SendMessage(sqsClient, finalMsgQueueName, doneMsg);
                    System.out.println("send done msg: " + doneMsg + " to queue: " + finalMsgQueueName);
                }
                sqsMethods.deleteMessage(finishedTasksQueueURL, finishedTask);
                System.out.println("delete msg: " + finishedTask + " from queue: " + finishedTasksQueueURL);
            } catch (Exception e)
            {
                System.out.println(e.getMessage());
            }
        }
    }

    public static void main(String[] argv) {
        System.out.println("Hello From Manager");
        Region region = Region.US_EAST_1;

        SQSMethods sqsMethods = SQSMethods.getInstance();

        Ec2Client ec2 = Ec2Client.builder()
                .region(region)
                .build();
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();

        final String queueNamePrefix = "lamqueue";
        String queueUrl = sqsMethods.getQueueUrl(queueNamePrefix);
        System.out.println("Local-Manager queue found!");

        String tasksQueueURL = createQueue(sqsClient, "tasksqueue");
        String finishedTasksQueueURL = createQueue(sqsClient, "finishedtasksqueue");

        System.out.println("is terminated: " + gotTerminate);

        new Thread(() -> {
            processFinishedTasks(s3, sqsClient, finishedTasksQueueURL);
        }, "ManagerWorkerGetDoneTasks").start();

        //to allow async processing of requests from different local applications
        ExecutorService pool = Executors.newFixedThreadPool(MAX_T);
        while(true)
        {
            Message msgToProcess = sqsMethods.receiveMessage(queueUrl);
            if(gotTerminationMsg(msgToProcess))
            {
                System.out.println("found termination msg - need to finish");
                gotTerminate = true;
                break;
            }

            ChangeMessageVisibilityRequest req = ChangeMessageVisibilityRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(msgToProcess.receiptHandle())
                    .visibilityTimeout(60) //1 min
                    .build();
            sqsClient.changeMessageVisibility(req);

            System.out.println("got new local app message to process");

            String[] msg = msgToProcess.body().split("\\$");
            int fileSize = Integer.parseInt(msg[msg.length - 1]);
            String finalMsgQueueName = msg[2];
            System.out.println("finalMsgQueueName: " + finalMsgQueueName + "filesize: " + fileSize);
            map.putIfAbsent(finalMsgQueueName, fileSize);
            Task task = new Task(ec2, sqsClient, s3, tasksQueueURL, msgToProcess, queueUrl);
            pool.execute(task);
        }

        //If got here - we got TERMINATE message - stopped looking for new tasks and to create summary files
        //TODO: 1. clean resources 2. kill workers 3. kill himself 4. terminate pool


    }
}
