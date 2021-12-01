import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.sqs.model.*;
import java.io.*;
import java.sql.Timestamp;
import java.util.*;

public class LocalApplication {

    public static int getFileSize(String fileName)
    {
        int fileSize = 0;
        try {
            File file = new File(fileName);
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null)
                    fileSize++;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return fileSize;
    }

    public static void createOutputFile(S3Methods s3Methods, Message finalMsg, String outputFileName)
    {
        try {
            String msgBody = finalMsg.body();
            String[] msg = msgBody.split("\\$");
            String bucketName = msg[0];
            String keyName = msg[1];

            File summeryFile = s3Methods.getS3ObjectAsFile(bucketName, keyName, "summeryFile"+ System.currentTimeMillis());
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
                    String outputFileLink = "https://" + bucketName + ".s3." + Region.US_EAST_1.toString().toLowerCase() + ".amazonaws.com/" + keyName;
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

    public static void main(String[] argv){
        try {
            if (argv.length < 3)
                return;
            String inputFileName = argv[0];
            String outFileName = argv[1];
            int n = Integer.parseInt(argv[2]);
            boolean terminate = argv.length > 3;

            EC2Methods ec2Methods = EC2Methods.getInstance();
            S3Methods s3Methods = S3Methods.getInstance();
            SQSMethods sqsMethods = SQSMethods.getInstance();

            String managerTagKey = "M";
            if(!ec2Methods.isEC2InstanceRunning(managerTagKey))
            {
                String ami = "ami-01cc34ab2709337aa";
                String tagValue = "EC2ManagerInstance";
                String instanceId = ec2Methods.createEC2Instance(managerTagKey, tagValue, ami, InstanceType.T2_MEDIUM);
                ec2Methods.startInstance(instanceId);
            }

            String bucketName = "localapplicationsbucket";
            if(!s3Methods.isBucketExist(bucketName))
                s3Methods.createBucket(bucketName);
            String key_name = "InputFile" + ProcessHandle.current().pid() + new Timestamp(System.currentTimeMillis());
            s3Methods.putS3Object(bucketName, key_name, inputFileName);

            String tasksQueueName = "lamqueue"; // only one queue for all local applications
            String finishQueueName = "malqueue" + ProcessHandle.current().pid() + System.currentTimeMillis(); // one queue for each local application
            String tasksQueueUrl = sqsMethods.getOrCreateQueue(tasksQueueName);
            String finishQueueUrl = sqsMethods.getOrCreateQueue(finishQueueName); // this queue will contain the finished msg per local application

            int inputFileSize = getFileSize(inputFileName);
            String msgBody = bucketName + "$" + key_name + "$" + finishQueueName + "$" + n + "$" + inputFileSize; // n - number of msg per worker
            sqsMethods.sendMessage(tasksQueueUrl, msgBody);

            Message finalMsg = sqsMethods.receiveMessage(finishQueueUrl);
            sqsMethods.deleteMessage(finishQueueUrl, finalMsg);

            createOutputFile(s3Methods, finalMsg, outFileName);

            if(terminate) {
                sqsMethods.sendMessage(tasksQueueUrl, "Terminate");
                s3Methods.deleteBucket(bucketName);
            }

            sqsMethods.deleteSQSQueue(finishQueueUrl);

        } catch (Exception exception)
        {
            System.out.println(exception.getMessage());
        }
    }
}
