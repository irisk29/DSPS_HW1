import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SQSMethods {
    private SqsClient sqsClient;

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

    // searching for queue with prefix queueName
    // if not find such one, create it
    public String getOrCreateQueue(String queueName)
    {
        try {
            ListQueuesRequest filterListRequest = ListQueuesRequest.builder()
                    .queueNamePrefix(queueName).build();
            ListQueuesResponse listQueuesFilteredResponse = sqsClient.listQueues(filterListRequest);
            List<String> urls = listQueuesFilteredResponse.queueUrls();
            if(urls.isEmpty()) //no queue found - creating one
            {
                String createQueueName = queueName + ".fifo"; //msg only sent once
                Map<String, String> attributes = new HashMap<>();
                attributes.put("FifoQueue", "true");
                attributes.put("ContentBasedDeduplication", "true");
                CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                        .queueName(createQueueName)
                        .attributesWithStrings(attributes)
                        .build();
                this.sqsClient.createQueue(createQueueRequest);

                GetQueueUrlResponse getQueueUrlResponse =
                        sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(createQueueName).build());
                String queueUrl = getQueueUrlResponse.queueUrl();
                System.out.println("create new queue with url: " + queueUrl);
                return queueUrl;
            }
            //should be only one for all local applications
            return urls.get(0);
        } catch (Exception exception) {
            System.out.println(exception.getMessage());
            return null;
        }
    }

    public void sendMessage(String queueUrl, String msgBody)
    {
        try {
            this.sqsClient.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageGroupId("dotask")
                    .messageBody(msgBody)
                    .build());
            System.out.println("message sent to manager: " + msgBody);
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
            this.sqsClient.deleteQueue(deleteQueueRequest);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }
}
