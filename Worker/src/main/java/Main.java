import Result.*;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueDoesNotExistException;
import java.sql.Timestamp;

public class Main {

    public static void main(String[] argv) {
        try {
            SQSMethods sqsMethods = SQSMethods.getInstance();
            S3Methods s3Methods = S3Methods.getInstance();
            final String tasksQueueName = "tasksqueue";
            final String finishedTasksQueueName = "finishedtasksqueue";
            final String finishedTasksQueueUrl = sqsMethods.getQueueUrl(finishedTasksQueueName);
            final String tasksQueueUrl = sqsMethods.getQueueTasksUrl(tasksQueueName);
            //for the case that the deletion is already started - we do not want to search for the task queue in infinite loop
            if(tasksQueueUrl == null)
            {
                String instanceId = EC2MetadataUtils.getInstanceId();
                EC2Methods ec2Methods = EC2Methods.getInstance();
                ec2Methods.stopInstance(instanceId);
            }

            while (true) {
                try
                {
                    Message message = sqsMethods.receiveMessage(tasksQueueUrl);
                    sqsMethods.changeMessageVisibility(tasksQueueUrl, message, 60); // 1 min to process
                    String[] msgData = message.body().split("\\t");
                    String action = msgData[0];
                    String pdfStringUrl = msgData[1];
                    String localAppID = msgData[2];

                    Result<String> resultFilePath = WorkerActions.doWorkerAction(action, pdfStringUrl);

                    String msgBody = "";
                    if(resultFilePath.getTag()) {
                        final String bucketName = "finishedtasksbucket";
                        final String objectKey = "outputFile" + ProcessHandle.current().pid() + new Timestamp(System.currentTimeMillis());
                        if(!s3Methods.isBucketExist(bucketName))
                            s3Methods.createBucket(bucketName);
                        s3Methods.putObject(bucketName, objectKey, resultFilePath.getValue());

                        // msg to manager
                        // prevPDFUrl $ s3 location (bucket name $ key name) $ action preformed $ localAppID for manager
                        msgBody = pdfStringUrl + "$" + bucketName + "$" + objectKey + "$" + action + "$" + localAppID;
                    }
                    else {
                        msgBody = "taskfailed$" + action + "$" + pdfStringUrl + "$" + localAppID + "$" + resultFilePath.getValue();
                    }

                    String messageGroupId = "finishedtask";
                    sqsMethods.sendMessage(finishedTasksQueueUrl, messageGroupId, msgBody);
                    sqsMethods.deleteMessage(tasksQueueUrl, message);
                }
                catch (QueueDoesNotExistException exception)
                {
                    String instanceId = EC2MetadataUtils.getInstanceId();
                    EC2Methods ec2Methods = EC2Methods.getInstance();
                    ec2Methods.stopInstance(instanceId);
                }
                catch (Exception e)
                {
                    System.out.println(e.getMessage());
                }
            }
        }
        catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }
}



