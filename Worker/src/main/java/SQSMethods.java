import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.NoSuchElementException;

public class SQSMethods {
    private static SQSMethods SQSMethods_instance = null;
    private final SqsClient sqsClient;

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

    // returns url of a queue if exists
    // if not, returns ""
    public String getQueueUrl(String queueNamePrefix)
    {
        List<String> urls;
        do {
            ListQueuesRequest filterListRequest = ListQueuesRequest.builder()
                    .queueNamePrefix(queueNamePrefix).build();
            ListQueuesResponse listQueuesFilteredResponse = this.sqsClient.listQueues(filterListRequest);
            urls = listQueuesFilteredResponse.queueUrls();
        } while(urls.isEmpty());
        // returns the lam queue url
        return urls.get(0);
    }

    public String getQueueTasksUrl(String queueNamePrefix)
    {
        ListQueuesRequest filterListRequest = ListQueuesRequest.builder()
                    .queueNamePrefix(queueNamePrefix).build();
        ListQueuesResponse listQueuesFilteredResponse = this.sqsClient.listQueues(filterListRequest);
        List<String> urls = listQueuesFilteredResponse.queueUrls();
        return urls.isEmpty() ? null : urls.get(0);
    }

    // sends a msgBody to queueUrl
    public void sendMessage(String queueUrl, String messageGroupId, String msgBody)
    {
        try {
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageGroupId(messageGroupId)
                    .messageBody(msgBody)
                    .build());
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    // waits until there is message to get from queueUrl
    // if there is more then one, returns the first
    public Message receiveMessage(String queueUrl) {
        List<Message> messages;
        do
        {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .build();
            messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
        } while (messages.isEmpty());
        // return only the first message each call
        return messages.get(0);
    }

    public void deleteMessage(String queueUrl, Message message) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(message.receiptHandle())
                .build();
        sqsClient.deleteMessage(deleteMessageRequest);
    }

    public void changeMessageVisibility(String queueUrl, Message msg, int seconds)
    {
        ChangeMessageVisibilityRequest req = ChangeMessageVisibilityRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(msg.receiptHandle())
                .visibilityTimeout(seconds)
                .build();
        sqsClient.changeMessageVisibility(req);
    }
}
