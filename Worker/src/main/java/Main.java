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
                String[] msgData = message.body().split("\\t");
                String action = msgData[0];
                String pdfStringUrl = msgData[1];
                String localAppID = msgData[2];

                String resultFilePath = WorkerActions.doWorkerAction(action, pdfStringUrl);

                final String bucketName = "finishedtasksbucket";
                final String objectKey = "outputFile" + ProcessHandle.current().pid() + new Timestamp(System.currentTimeMillis());
                if(!s3Methods.isBucketExist(bucketName))
                    s3Methods.createBucket(bucketName);
                s3Methods.putObject(bucketName, objectKey, resultFilePath);

                String messageGroupId = "finishedtask";
                // msg to manager
                // prevPDFUrl $ s3 location (bucket name $ key name) $ action preformed
                String msgBody = pdfStringUrl + "$" + bucketName + "$" + objectKey + "$" + action + "$" + localAppID;
                sqsMethods.sendMessage(finishedTasksQueueUrl, messageGroupId, msgBody);

                sqsMethods.deleteMessage(tasksQueueUrl, message);
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }
}



