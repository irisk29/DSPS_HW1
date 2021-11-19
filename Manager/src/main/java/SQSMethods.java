import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

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

    // waits until there is message to get from queueUrl
    // if there is more then one, returns the first
    public List<Message> receiveBatchMessages(String queueUrl) {
        List<Message> messages;
        do
        {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20)
                    .build();
            messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
        } while (messages.isEmpty());
        // return only the first message each call
        return messages;
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
