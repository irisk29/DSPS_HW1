import Result.*;
import software.amazon.awssdk.services.sqs.model.Message;
import java.sql.Timestamp;

public class Main {

    public static void main(String[] argv) {
        try {
            SQSMethods sqsMethods = SQSMethods.getInstance();
            S3Methods s3Methods = S3Methods.getInstance();
            final String tasksQueueName = "tasksqueue";
            final String finishedTasksQueueName = "finishedtasksqueue";
            final String tasksQueueUrl = sqsMethods.getQueueUrl(tasksQueueName);
            final String finishedTasksQueueUrl = sqsMethods.getQueueUrl(finishedTasksQueueName);

            while (true) {
                Message message = sqsMethods.receiveMessage(tasksQueueUrl);
                System.out.println("receive task from manager");
                String[] msgData = message.body().split("\\t");
                String action = msgData[0];
                String pdfStringUrl = msgData[1];
                String localAppID = msgData[2];

                System.out.println("action: " + action + "pdfStringUrl: " + pdfStringUrl + "localAppID: " + localAppID);
                Result<String> resultFilePath = WorkerActions.doWorkerAction(action, pdfStringUrl);
                System.out.println("finish do task");

                String msgBody = "";
                if(resultFilePath.getTag()) {
                    final String bucketName = "finishedtasksbucket";
                    final String objectKey = "outputFile" + ProcessHandle.current().pid() + new Timestamp(System.currentTimeMillis());
                    if(!s3Methods.isBucketExist(bucketName))
                        s3Methods.createBucket(bucketName);
                    s3Methods.putObject(bucketName, objectKey, resultFilePath.getValue());
                    System.out.println("put new file: " + resultFilePath + " to bucketName: " + bucketName + " objectKey: " + objectKey);

                    // msg to manager
                    // prevPDFUrl $ s3 location (bucket name $ key name) $ action preformed $ localAppID for manager
                    msgBody = pdfStringUrl + "$" + bucketName + "$" + objectKey + "$" + action + "$" + localAppID;
                }
                else {
                    msgBody = "taskfailed$" + action + "$" + pdfStringUrl + "$" + localAppID + "$" + resultFilePath.getValue();
                }

                String messageGroupId = "finishedtask";
                sqsMethods.sendMessage(finishedTasksQueueUrl, messageGroupId, msgBody);
                System.out.println("send message to manager: " + msgBody + " to queue url: " + finishedTasksQueueUrl);

                sqsMethods.deleteMessage(tasksQueueUrl, message);
                System.out.println("deleted msg from queue: " + tasksQueueUrl);
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }
}



