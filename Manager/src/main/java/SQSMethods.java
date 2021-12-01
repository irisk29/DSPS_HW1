import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SQSMethods {

    private final SqsClient sqsClient;

    private SQSMethods() {
        Region region = Region.US_EAST_1;
        this.sqsClient = SqsClient.builder()
                .region(region)
                .build();
    }

    private static class Holder { // thread-safe singleton
        private static final SQSMethods INSTANCE = new SQSMethods();
    }

    public static SQSMethods getInstance() {
        return SQSMethods.Holder.INSTANCE;
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
    public Message receiveMessage(String queueUrl, AtomicBoolean gotTerminated) {
        List<Message> messages;
        do
        {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(1)
                    .build();
            messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
        } while (messages.isEmpty() && !gotTerminated.get());
        // return only the first message each call
        return messages.isEmpty() ? null : messages.get(0);
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
        }
    }

    public void deleteSQSQueue(String queueUrl) {

        try {
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqsClient.deleteQueue(deleteQueueRequest);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    public String createQueue(String queueNameToCreate)
    {
        try {
            String queueName = queueNameToCreate + System.currentTimeMillis() + ".fifo"; //msg only sent once
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
            return queueUrl;
        }catch (Exception exception)
        {
            System.out.println(exception.getMessage());
        }
        return "";
    }

    public void SendMessage(String queueUrl , String msgBody)
    {
        try {
            sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageGroupId("finishedtask")
                    .messageBody(msgBody)
                    .build());

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
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

    public void sendBatchMessage(String queueUrl, Collection<SendMessageBatchRequestEntry> messages)
    {
        SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(messages)
                .build();
        sqsClient.sendMessageBatch(sendMessageBatchRequest);
    }
}
