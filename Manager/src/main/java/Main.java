import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
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

    public static List<Message> receiveMessages(SqsClient sqsClient, String queueUrl) {

        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .build();
            return sqsClient.receiveMessage(receiveMessageRequest).messages();
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public static boolean gotTerminationMsg(List<Message> messages)
    {
        for(Message msg : messages)
        {
            if(msg.body().equals("Terminate"))
                return true;
        }
        return false;
    }

    public static String findLocalAppManagerQueue(SqsClient sqsClient)
    {
        final String namePrefix = "lamqueue";
        ListQueuesRequest filterListRequest = ListQueuesRequest.builder()
                .queueNamePrefix(namePrefix).build();

        ListQueuesResponse listQueuesFilteredResponse = sqsClient.listQueues(filterListRequest);
        System.out.println("Queue URLs with prefix: " + namePrefix);
        List<String> urls = listQueuesFilteredResponse.queueUrls();
        return urls.isEmpty() ? "" : urls.get(0);
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
        S3Methods s3Methods = S3Methods.getInstance();
        SQSMethods sqsMethods = SQSMethods.getInstance();

        while(gotTerminate)
        {
            Message finishedTask = receiveMessages(sqsClient, finishedTasksQueueURL).get(0); //reads only 1 msg
            String[] msgBody = finishedTask.body().split("\\$");
            String original_pdf_url = msgBody[0];
            String bucketName = msgBody[1];
            String keyName = msgBody[2];
            String operation = msgBody[3];
            String finalMsgQueueName = msgBody[4];
            int fileSize = map.get(finalMsgQueueName);
            try
            {
                File f = new File(finalMsgQueueName + ".txt");
                PrintWriter out = new PrintWriter(new FileWriter(f, true));
                String result = original_pdf_url + "$" + bucketName + "$" + keyName + "$" + operation;
                out.println(result);
                out.close();
                int currFileSize = getFileSize(f);
                if(currFileSize == fileSize) //finished all the tasks for certain local application
                {
                    String bucket_name = "finishedtasksbucket";
                    if(!s3Methods.isBucketExist(bucket_name))
                        s3Methods.createBucket(bucket_name);
                    String key_name = finalMsgQueueName + System.currentTimeMillis();
                    s3Methods.putObject(bucketName, key_name, f); //uploading the summary file to s3

                    String doneMsg = bucket_name + "$" + key_name; //location of the summary file in s3
                    SendMessage(sqsClient, finishedTasksQueueURL, doneMsg);
                }
                sqsMethods.deleteMessage(finishedTasksQueueURL, finishedTask);
            }catch (Exception e)
            {
                System.out.println(e.getMessage());
            }
        }
    }

    public static void main(String[] argv) {
        System.out.println("Hello From Manager");
        Region region = Region.US_EAST_1;

        Ec2Client ec2 = Ec2Client.builder()
                .region(region)
                .build();
        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .build();

        String queueUrl = findLocalAppManagerQueue(sqsClient);
        if(queueUrl.equals("")){
            System.out.println("No Local-Manager queue found!");
            return;
        }

        String tasksQueueURL = createQueue(sqsClient, "tasksqueue");
        String finishedTasksQueueURL = createQueue(sqsClient, "finishedtasksqueue");

        new Thread(() -> {
            processFinishedTasks(s3, sqsClient, finishedTasksQueueURL);
        }, "ManagerWorkerGetDoneTasks").start();

        //to allow async processing of requests from different local applications
        ExecutorService pool = Executors.newFixedThreadPool(MAX_T);
        while(true)
        {
            List<Message> messages = receiveMessages(sqsClient, queueUrl);
            if(gotTerminationMsg(messages))
            {
                System.out.println("found termination msg - need to finish");
                gotTerminate = true;
                break;
            }
            Message msgToProcess = messages.get(0);
            String[] msg = msgToProcess.body().split("\\$");
            int fileSize = Integer.parseInt(msg[msg.length - 1]);
            String finalMsgQueueName = msg[2];
            map.putIfAbsent(finalMsgQueueName, fileSize);
            Task task = new Task(ec2, sqsClient, s3, tasksQueueURL, msgToProcess);
            pool.execute(task);
        }

        //If got here - we got TERMINATE message - stopped looking for new tasks and to create summary files
        //TODO: 1. clean resources 2. kill workers 3. kill himself 4. terminate pool
        

    }
}
