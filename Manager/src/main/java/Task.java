import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.util.List;

public class Task implements Runnable{
    Message task;
    Ec2Client ec2;
    SqsClient sqsClient;
    S3Client s3;
    String tasksQueueURL;
    static int counter = 0;

    public Task(Ec2Client ec2 , SqsClient sqsClient, S3Client s3, String tasksQueueURL, Message task)
    {
        this.task = task;
        this.ec2 = ec2;
        this.s3 = s3;
        this.sqsClient = sqsClient;
        this.tasksQueueURL = tasksQueueURL;
    }

    public static void createTasksAndWorkers(Ec2Client ec2 , SqsClient sqsClient, S3Client s3, Message taskMsg,
                                             String tasksQueueURL)
    {
        String task = taskMsg.body();
        String[] taskBody = task.split("\\$");
        String bucketName = taskBody[0];
        String key_name = taskBody[1];
        String finalMsgQueueName = taskBody[2];
        String numMsgPerWorker = taskBody[3];

        int fileSize = readTaskFile(sqsClient, s3, bucketName, key_name, tasksQueueURL, finalMsgQueueName);
        int numOfNeededWorkers = fileSize / Integer.parseInt(numMsgPerWorker);

        createWorkersPerTasks(ec2, numOfNeededWorkers);
    }

    public static void createWorkersPerTasks(Ec2Client ec2, int numOfNeededWorkers)
    {
        int numOfCurrWorkers = findRunningWorkersEC2Instances(ec2);
        int numOfWorkersToCreate = Math.min(numOfNeededWorkers , 20 - 1 - 1) - numOfCurrWorkers;
        //20 - 1 - 1: max allowed is 19 and 1 for manager
        if(numOfWorkersToCreate <= 0) return; //no need to create

        String ami = "ami-01cc34ab2709337aa";
        String name = "EC2ManagerInstance";
        createAndStartEC2WorkerInstance(ec2, name, ami, numOfWorkersToCreate);

    }

    public static void createAndStartEC2WorkerInstance(Ec2Client ec2,String name, String amiId, int maxCount ) {

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(maxCount)
                .minCount(1)
                //.iamInstanceProfile(IamInstanceProfileSpecification.builder().arn("arn:aws:iam::935282201937:instance-profile/default").build())
                /*.userData("#!/bin/sh\n" +
                        "sudo yum install java-1.8.0-openjdk\n" +
                        "aws s3 cp s3://bucket/folder/manager.jar .\n" +
                        "java -jar /home/ubuntu/manager.jar")*/
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        List<Instance> instances = response.instances();

        Tag tag = Tag.builder()
                .key("W ") //for worker
                .value(name)
                .build();

        for(Instance instance: instances)
        {
            String instanceId = instance.instanceId();
            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();
            try {
                ec2.createTags(tagRequest);
                System.out.printf(
                        "Successfully started EC2 Worker Instance %s based on AMI %s\n",
                        instanceId, amiId);

            } catch (Ec2Exception e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                System.exit(1);
            }

            startInstance(ec2, instanceId);
        }
    }

    public static void startInstance(Ec2Client ec2, String instanceId) {

        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.startInstances(request);
        System.out.printf("Successfully started instance %s\n", instanceId);
    }

    public static int findRunningWorkersEC2Instances(Ec2Client ec2) {
        int numOfWorkers = 0;
        try {
            String nextToken = null;

            do {
                Filter filter = Filter.builder()
                        .name("instance-state-name")
                        .values("running")
                        .build();

                DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                        .filters(filter)
                        .build();

                DescribeInstancesResponse response = ec2.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        System.out.printf(
                                "Found Reservation with id %s, " +
                                        "AMI %s, " +
                                        "type %s, " +
                                        "state %s \n",
                                instance.instanceId(),
                                instance.imageId(),
                                instance.instanceType(),
                                instance.state().name());
                        for(Tag tag : instance.tags())
                        {
                            if(tag.key().equals("W")) //found worker
                            {
                                System.out.println("Found worker instance!");
                                numOfWorkers++;
                            }
                        }
                        System.out.println("");
                    }
                }
                nextToken = response.nextToken();

            } while (nextToken != null);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return  numOfWorkers;
    }

    public static File getObjectBytes (S3Client s3, String bucketName, String keyName, String outputFile ) {

        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
            byte[] data = objectBytes.asByteArray();

            // Write the data to a local file
            File myFile = new File(outputFile);
            OutputStream os = new FileOutputStream(myFile);
            os.write(data);
            System.out.println("Successfully obtained bytes from an S3 object");
            os.close();
            return myFile;
        } catch (IOException ex) {
            ex.printStackTrace();
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return null;
    }

    public static void SendMessage(SqsClient sqsClient, String queueUrl , String msgBody)
    {
        try {
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageGroupId("dotask")
                    .messageBody(msgBody)
                    .build());

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static int readTaskFile(SqsClient sqsClient, S3Client s3, String bucketName, String key_name,
                                   String queueUrl, String finalMsgQueueName)
    {
        File file = getObjectBytes(s3, bucketName, key_name, "Task" + counter + ".txt");
        counter++;
        int fileSize = 0;
        try {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    //finalMsgQueueName - to know to which local app we are doing the task for
                    String msgBody = line + "\t" + finalMsgQueueName;
                    SendMessage(sqsClient, queueUrl, msgBody);
                    fileSize++;
                }
            }
            return fileSize;
        }catch (Exception exception)
        {
            System.out.println(exception.getMessage());
        }
        return 0;
    }

    public static void deleteMessage(SqsClient sqsClient, String queueUrl,  Message message) {
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteMessageRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
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
        createTasksAndWorkers(ec2,sqsClient, s3, task, tasksQueueURL);
        deleteMessage(sqsClient, tasksQueueURL, task); //done processing the request from localapp
    }
}
