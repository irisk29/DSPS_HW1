import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.StartInstancesRequest;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import org.apache.commons.codec.binary.Base64;
import javafx.util.Pair;


import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.*;

public class LocalApplication {
    final public static String IAM_ROLE = "arn:aws:iam::935282201937:instance-profile/LabInstanceProfile";
    final public static String SECURITY_ID = "sg-03e1043c7ed636b1a";
    final public static String KEY_NAME = "dsps";

    public static void createEC2KeyPair(Ec2Client ec2, String keyName) {

        try {
            CreateKeyPairRequest request = CreateKeyPairRequest.builder()
                    .keyName(keyName).build();

            ec2.createKeyPair(request);
            System.out.printf(
                    "Successfully created key pair named %s",
                    keyName);

        } catch (Ec2Exception ignored) {}
    }

    public static String createEC2ManagerInstance(Ec2Client ec2,String name, String amiId ) {

        createEC2KeyPair(ec2, KEY_NAME);
        String userData = """
                #!/bin/sh
                rpm --import https://yum.corretto.aws/corretto.key
                curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo
                yum install -y java-15-amazon-corretto-devel
                aws s3 cp s3://workermanagerjarsbucket/Manager.jar .
                cd /
                java -jar Manager.jar""";
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MEDIUM)
                .maxCount(1)
                .minCount(1)
                .securityGroupIds(SECURITY_ID)
                .keyName(KEY_NAME)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn(IAM_ROLE).build())
                .userData(Base64.encodeBase64String(userData.getBytes(StandardCharsets.UTF_8)))
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        Tag tag = Tag.builder()
                .key("M") //for manager
                .value(name)
                .build();

        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf(
                    "Successfully started EC2 Instance %s based on AMI %s\n",
                    instanceId, amiId);

            return instanceId;

        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }

        return "";
    }

    public static void startInstance(Ec2Client ec2, String instanceId) {

        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2.startInstances(request);
        System.out.printf("Successfully started instance %s\n", instanceId);
    }

    // checks if the bucketName already been created
    public static boolean isBucketExist(S3Client s3Client, String bucketName) {
        HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();
        try {
            s3Client.headBucket(headBucketRequest);
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    public static void createBucket( S3Client s3Client, String bucketName) {

        try {
            S3Waiter s3Waiter = s3Client.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Client.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            // Wait until the bucket is created and print out the response.
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println(bucketName +" is ready");

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }


    // Return a byte array
    private static byte[] getObjectFile(String filePath) {

        FileInputStream fileInputStream = null;
        byte[] bytesArray = null;

        try {
            File file = new File(filePath);
            bytesArray = new byte[(int) file.length()];
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytesArray);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bytesArray;
    }

    public static String putS3Object(S3Client s3,
                                     String bucketName,
                                     String objectKey,
                                     String objectPath) {

        try {

            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build();

            PutObjectResponse response = s3.putObject(putOb,
                    RequestBody.fromBytes(getObjectFile(objectPath)));

            return response.eTag();

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return "";
    }

    public static boolean findRunningManagerEC2Instances(Ec2Client ec2) {

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
                            if(tag.key().equals("M")) //found manager
                            {
                                System.out.println("Found manager instance!");
                                return true;
                            }
                        }
                        System.out.println("");
                    }
                }
                nextToken = response.nextToken();

            } while (nextToken != null);
            return false;

        } catch (Ec2Exception e) {
            return false;
        }
    }

    public static String GetOrCreateTasksQueue(SqsClient sqsClient)
    {
        try {
            String queueUrl = "";
            String namePrefix = "lamqueue";
            ListQueuesRequest filterListRequest = ListQueuesRequest.builder()
                    .queueNamePrefix(namePrefix).build();

            ListQueuesResponse listQueuesFilteredResponse = sqsClient.listQueues(filterListRequest);
            System.out.println("Queue URLs with prefix: " + namePrefix);
            List<String> urls = listQueuesFilteredResponse.queueUrls();
            if(urls.isEmpty()) //no queue found - creating one
            {
                String queueName = namePrefix + System.currentTimeMillis() + ".fifo"; //msg only sent once
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
                queueUrl = getQueueUrlResponse.queueUrl();
                System.out.println("queue url: " + queueUrl);
                return queueUrl;
            }

            return urls.get(0); //should be only one for all local applications
        }catch (Exception exception)
        {
            System.out.println(exception.getMessage());
        }
        return "";
    }

    //this queue will contain the finished msg per local application
    public static Pair<String,String> CreateFinishQueue(SqsClient sqsClient)
    {
        try {
            String queueName = "malqueue" + ProcessHandle.current().pid() + System.currentTimeMillis() + ".fifo"; //msg only sent once
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
            System.out.println("queue url: " + queueUrl);
            return new Pair<>(queueUrl, queueName);

        }catch (Exception exception)
        {
            System.out.println(exception.getMessage());
        }
        return null;
    }

    public static void SendMessage(SqsClient sqsClient, String queueUrl , String msgBody)
    {
        try {
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageGroupId("dotask")
                    .messageBody(msgBody) //bucketname+key_name to find the input file; the queuename is for the manger to know where to put the result
                    .build());
            System.out.println("message sent to manager: " + msgBody);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    public static  List<Message> receiveMessages(SqsClient sqsClient, String queueUrl) {

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

    public static Message getFinalMsg(SqsClient sqsClient, String queueUrl)
    {
        while (true)
        {
            List<Message> messages = receiveMessages(sqsClient, queueUrl);
            if(!messages.isEmpty()) //got the final msg
            {
                Message msg = messages.get(0);
                deleteMessage(sqsClient,queueUrl,msg);
                return msg; //should be only one
            }
        }
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

    public static URL getURL(S3Client s3, String bucketName, String keyName) {
        try {
            GetUrlRequest request = GetUrlRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();

            return s3.utilities().getUrl(request);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return null;
        }
    }

    public static void createOutputFile(S3Client s3, Message finalMsg, String outputFileName)
    {
        try {
            String msgBody = finalMsg.body();
            String[] msg = msgBody.split("\\$");
            String bucketName = msg[0];
            String keyName = msg[1];

            File summeryFile = getObjectBytes(s3, bucketName, keyName, "summeryFile"+ System.currentTimeMillis());
            Scanner scanner = new Scanner(summeryFile);
            scanner.useDelimiter("@");

            File outputFile = new File(outputFileName + System.currentTimeMillis() + ".html");
            PrintWriter writer = new PrintWriter(outputFile);
            writer.write("<html><head><title>Final Output</title></head><body><p>");
            String[] msgLine;
            String operation, inputFileLink, data = "";
            while(scanner.hasNext()) {
                String line = scanner.next();
                if(!line.contains("$"))
                    break;
                msgLine = line.split("\\$");
                operation = msgLine[0];
                inputFileLink = msgLine[1];
                // successful operation - need to download the output file
                if(msgLine.length > 3) {
                    bucketName = msgLine[2];
                    keyName = msgLine[3];
                    String outputFileLink = Objects.requireNonNull(getURL(s3, bucketName, keyName)).getPath();
                    data = operation + ": <a href=\"" + inputFileLink + "\">Input File</a> <a href=\"" + outputFileLink + "\">Output File</a><br>";
                }
                // unsuccessful operation - need to attach the error message
                else {
                    String errorDesc = msgLine[2];
                    data = operation + ": <a href=\"" + inputFileLink + "\">Input File</a> " + errorDesc + "<br>";
                }
                writer.write(data);
            }
            writer.write("</p></body></html>");
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int getFileSize(String fileName)
    {
        int fileSize = 0;
        try {
            File file = new File(fileName);
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

    public static void deleteSQSQueue(String queueUrl) {

        try {
            SqsClient sqsClient = SqsClient.builder()
                    .region(Region.US_EAST_1)
                    .build();

            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqsClient.deleteQueue(deleteQueueRequest);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public static void deleteBucket(String bucket) {

        try {
            S3Client s3Client = S3Client.builder()
                    .region(Region.US_EAST_1)
                    .build();
            // To delete a bucket, all the objects in the bucket must be deleted first
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).build();
            ListObjectsV2Response listObjectsV2Response;

            do {
                listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    s3Client.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key(s3Object.key())
                            .build());
                }

                listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket)
                        .continuationToken(listObjectsV2Response.nextContinuationToken())
                        .build();

            } while(listObjectsV2Response.isTruncated());

            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
            s3Client.deleteBucket(deleteBucketRequest);

        } catch (S3Exception ignored) {}
    }

    public static void main(String[] argv){
        try {
            if (argv.length < 3)
                return;
            String inputFileName = argv[0];
            String outFileName = argv[1];
            int n = Integer.parseInt(argv[2]);
            boolean terminate = argv.length > 3;

            String ami = "ami-01cc34ab2709337aa";
            String name = "EC2ManagerInstance";
            Region region = Region.US_EAST_1;

            int inputFileSize = getFileSize(inputFileName);

            Ec2Client ec2 = Ec2Client.builder()
                    .region(region)
                    .build();

            if(!findRunningManagerEC2Instances(ec2))
            {
                String instanceId = createEC2ManagerInstance(ec2,name,ami);
                startInstance(ec2, instanceId);
            }

            S3Client s3 = S3Client.builder()
                .region(region)
                .build();
            String bucketName = "localapplicationirissagiv";
            if(!isBucketExist(s3,bucketName))
                createBucket(s3, bucketName);
            String key_name = "InputFile" + ProcessHandle.current().pid() + new Timestamp(System.currentTimeMillis());
            putS3Object(s3, bucketName, key_name, inputFileName);

            SqsClient sqsClient = SqsClient.builder()
                    .region(region)
                    .build();

            String tasksQueueUrl = GetOrCreateTasksQueue(sqsClient);
            Pair<String,String> finishQueuePair = CreateFinishQueue(sqsClient); //which queue the manager needs to put the final msg
            assert finishQueuePair != null;
            String finishQueueUrl = finishQueuePair.getKey();
            String finishQueueName = finishQueuePair.getValue();
            String msgBody = bucketName + "$" + key_name + "$" + finishQueueName + "$" + n + "$" + inputFileSize; //n - number of msg per worker
            SendMessage(sqsClient,tasksQueueUrl,msgBody);

            Message finalMsg = getFinalMsg(sqsClient, finishQueueUrl);
            System.out.println("got final msg");
            createOutputFile(s3, finalMsg, outFileName);
            if(terminate)
            {
                SendMessage(sqsClient, tasksQueueUrl, "Terminate");
            }
            deleteSQSQueue(finishQueueUrl);
            deleteBucket(bucketName);
        } catch (Exception exception)
        {
            System.out.println(exception.getMessage());
        }
    }
}
