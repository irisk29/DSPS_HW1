import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class Task implements Runnable{
    Message task;
    String tasksQueueURL;
    String lamQueueURL;

    static long counter = 0;
    static long batchCounter = 0;

    public Task(String tasksQueueURL, Message task, String lamQueueURL)
    {
        this.task = task;
        this.tasksQueueURL = tasksQueueURL;
        this.lamQueueURL = lamQueueURL;
    }

    public String generateString() {
        return UUID.randomUUID().toString();
    }

    public void createTasksAndWorkers(Message taskMsg, String tasksQueueURL)
    {
        String task = taskMsg.body();
        String[] taskBody = task.split("\\$");
        String bucketName = taskBody[0];
        String key_name = taskBody[1];
        String finalMsgQueueName = taskBody[2];
        String numMsgPerWorker = taskBody[3];
        System.out.println(bucketName + " " + key_name + " " + finalMsgQueueName + " " + numMsgPerWorker);

        int fileSize = sendTaskMessages(bucketName, key_name, tasksQueueURL, finalMsgQueueName);
        int numOfNeededWorkers = (int) Math.ceil(fileSize / Double.parseDouble(numMsgPerWorker));

        createWorkersPerTasks(numOfNeededWorkers);
    }

    public void createWorkersPerTasks(int numOfNeededWorkers)
    {
        EC2Methods ec2Client = EC2Methods.getInstance();
        int numOfCurrWorkers = ec2Client.findWorkersByState("running").size();
        int numOfWorkersToCreate = Math.min(numOfNeededWorkers , 20 - 1 - 1) - numOfCurrWorkers;
        //20 - 1 - 1: max allowed is 19 and 1 for manager
        if(numOfWorkersToCreate <= 0) {
            System.out.println("No need to create more workers!");
            return; //no need to create
        }
        System.out.println("need to create " + numOfWorkersToCreate + " more workers!");
        String ami = "ami-01cc34ab2709337aa";
        String name = "EC2WorkerInstance";
        ec2Client.createAndStartEC2WorkerInstance(name, ami, numOfWorkersToCreate);
    }

    public void sendBatchMessages(String queueUrl, List<String> msgsBody) {
        try {
            SQSMethods sqsMethods = SQSMethods.getInstance();
            Collection<SendMessageBatchRequestEntry> messages = new ArrayList<>();
            int id = 0;
            for(String msgBody : msgsBody)
            {
                String msgId = generateString();
                String mi = id + "-" + msgId + "-" + batchCounter;
                messages.add(SendMessageBatchRequestEntry.builder().id(msgId).messageBody(msgBody)
                        .messageGroupId("finishedtask-" + mi).messageDeduplicationId("finishedtaskdup-" + mi).build());
                id++;
            }
            batchCounter++;
            sqsMethods.sendBatchMessage(queueUrl, messages);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }


    public int sendTaskMessages(String bucketName, String key_name, String queueUrl, String finalMsgQueueName)
    {
        S3Methods s3Methods = S3Methods.getInstance();

        File file = s3Methods.getObjectBytes(bucketName, key_name, "Task" + counter + ".txt");
        System.out.println("create file in manager");
        counter++;
        int fileSize = 0;
        List<String> messages = new ArrayList<>();
        try {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if(messages.size() == 10) {
                        sendBatchMessages(queueUrl, messages);
                        messages.clear();
                    }
                    //finalMsgQueueName - to know to which local app we are doing the task for
                    String msgBody = line + "\t" + finalMsgQueueName;
                    messages.add(msgBody);
                    //SendMessage(sqsClient, queueUrl, msgBody);
                    fileSize++;
                }
            }
            if(!messages.isEmpty())
                sendBatchMessages(queueUrl, messages);

            System.out.println("file size is: " + fileSize);
            return fileSize;
        }catch (Exception exception)
        {
            System.out.println(exception.getMessage());
        }
        return 0;
    }

    /**
     * When an object implementing interface {@code Runnable} is used
     * to create a thread, starting the thread causes the object's
     * {@code run} method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method {@code run} is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        try {
            SQSMethods sqsClient = SQSMethods.getInstance();
            createTasksAndWorkers(task, tasksQueueURL);
            sqsClient.deleteMessage(lamQueueURL, task); //done processing the request from localapp
        } catch (Exception ignored)
        {}
    }
}
