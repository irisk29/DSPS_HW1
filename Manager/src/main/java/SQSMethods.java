import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class SQSMethods {
    private static SQSMethods SQSMethods_instance = null;
    private SqsClient sqsClient;

    private SQSMethods() {
        Region region = Region.US_EAST_1;
        this.sqsClient = SqsClient.builder()
                .region(region)
                .build();
    }

    public static SQSMethods getInstance()
    {
        if (SQSMethods_instance == null)
            SQSMethods_instance = new SQSMethods();
        return SQSMethods_instance;
    }

    public void deleteMessage(String queueUrl,  Message message) {
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            this.sqsClient.deleteMessage(deleteMessageRequest);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
