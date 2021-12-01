import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class Main {

    static AtomicBoolean gotTerminate = new AtomicBoolean(false);
    final static int MAX_T = 10;
    static ConcurrentHashMap<String, Integer> map
            = new ConcurrentHashMap<>();

    public static boolean gotTerminationMsg(Message message)
    {
        return message == null || message.body().equals("Terminate");
    }

    public static int getFileSize(File file)
    {
        int fileSize = 0;
        try {
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                String line;
                while ((line = br.readLine()) != null) {
                    if(line.contains("@")) //every finished task will end with this symbol
                        fileSize++;
                }
            }
        }catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
        return fileSize;
    }

    /*message from worker will be in the format - <original_pdf_url, the S3 url of the new
    image file, operation that was performed, finalMsgQueueName>
    where finalMsgQueueName represents the queue name between local application and the manager*/
    public static void processFinishedTasks(String finishedTasksQueueURL)
    {
        System.out.println("In start of processFinishedTasks");
        S3Methods s3Methods = S3Methods.getInstance();
        SQSMethods sqsMethods = SQSMethods.getInstance();

        while(!gotTerminate.get())
        {
            Message finishedTask = sqsMethods.receiveMessage(finishedTasksQueueURL, gotTerminate); //reads only 1 msg
            if(finishedTask == null)
                break;
            System.out.println("got finished task");
            String[] msgBody = finishedTask.body().split("\\$");
            System.out.println("msg body is: " + Arrays.toString(msgBody));
            String result = "", finalMsgQueueName = "";
            if(msgBody[0].equals("taskfailed")) {
                // action + "$" + pdfStringUrl + "$" + localAppID + "$" + resultFilePath.getValue();
                String action = msgBody[1];
                String pdfStringUrl = msgBody[2];
                finalMsgQueueName = msgBody[3];
                String exceptionDesc = msgBody[4];
                result = action + "$" + pdfStringUrl + "$" + exceptionDesc + "@";
            }
            else {
                String original_pdf_url = msgBody[0];
                String bucketName = msgBody[1];
                String keyName = msgBody[2];
                String operation = msgBody[3];
                finalMsgQueueName = msgBody[4];
                result = operation + "$" + original_pdf_url + "$" + bucketName + "$" + keyName + "@";
            }

            System.out.println("result: " + result + " finalMsgQueueName: " + finalMsgQueueName);

            int fileSize = map.get(finalMsgQueueName);
            try
            {
                File f = new File(finalMsgQueueName + ".txt");
                PrintWriter out = new PrintWriter(new FileWriter(f, true));
                out.println(result);
                out.close();
                int currFileSize = getFileSize(f);
                if(currFileSize == fileSize) //finished all the tasks for certain local application
                {
                    EC2Methods.log.debug("finished all the tasks for local app " + finalMsgQueueName);
                    String bucket_name = "finishedtasksbucket";
                    if(!s3Methods.isBucketExist(bucket_name))
                        s3Methods.createBucket(bucket_name);
                    String key_name = finalMsgQueueName + System.currentTimeMillis();
                    s3Methods.putObject(bucket_name, key_name, f); //uploading the summary file to s3

                    String doneMsg = bucket_name + "$" + key_name; //location of the summary file in s3
                    String finalMsgQueueUrl = sqsMethods.getQueueUrl(finalMsgQueueName);
                    sqsMethods.SendMessage(finalMsgQueueUrl, doneMsg);
                    System.out.println("send done msg: " + doneMsg + " to queue: " + finalMsgQueueUrl);
                    EC2Methods.log.debug("send done msg: " + doneMsg + " to queue: " + finalMsgQueueUrl);
                }
                sqsMethods.deleteMessage(finishedTasksQueueURL, finishedTask);
                System.out.println("delete msg: " + finishedTask + " from queue: " + finishedTasksQueueURL);
            } catch (Exception e)
            {
                System.out.println(e.getMessage());
            }
        }
    }

    public static void main(String[] argv) {
        System.out.println("Hello From Manager");

        SQSMethods sqsMethods = SQSMethods.getInstance();
        EC2Methods ec2Methods = EC2Methods.getInstance();

        final String queueNamePrefix = "lamqueue";
        String queueUrl = sqsMethods.getQueueUrl(queueNamePrefix);
        System.out.println("Local-Manager queue found!");
        EC2Methods.log.debug("Local-Manager queue found!");

        String tasksQueueURL = sqsMethods.createQueue("tasksqueue");
        String finishedTasksQueueURL = sqsMethods.createQueue("finishedtasksqueue");

        System.out.println("is terminated: " + gotTerminate);

        Thread buildFinalFileThread = new Thread(() -> {
            processFinishedTasks(finishedTasksQueueURL);
        }, "ManagerWorkerGetDoneTasks");
        buildFinalFileThread.start();

        //to allow async processing of requests from different local applications
        ExecutorService pool = Executors.newFixedThreadPool(MAX_T);
        while(true)
        {
            Message msgToProcess = sqsMethods.receiveMessage(queueUrl, gotTerminate);
            if(gotTerminationMsg(msgToProcess))
            {
                System.out.println("found termination msg - need to finish");
                EC2Methods.log.debug("found termination msg - need to finish");
                break;
            }

           sqsMethods.changeMessageVisibility(queueUrl, msgToProcess, 30 * 60); // 30 min

            System.out.println("got new local app message to process");
            EC2Methods.log.debug("got new local app message to process");

            String[] msg = msgToProcess.body().split("\\$");
            int fileSize = Integer.parseInt(msg[msg.length - 1]);
            String finalMsgQueueName = msg[2];
            System.out.println("finalMsgQueueName: " + finalMsgQueueName + "filesize: " + fileSize);
            map.putIfAbsent(finalMsgQueueName, fileSize);
            Task task = new Task(tasksQueueURL, msgToProcess, queueUrl);
            pool.execute(task);
        }

        //If got here - we got TERMINATE message - stopped looking for new tasks and to create summary files
        // 1. terminate pool 2. clean resources 3. kill workers 4. kill himself
        int totalWorkers = ec2Methods.findWorkersByState("running").size();
        totalWorkers += ec2Methods.findWorkersByState("pending").size();
        pool.shutdownNow();
        sqsMethods.deleteSQSQueue(tasksQueueURL); // no more tasks will be sent to the workers
        sqsMethods.deleteSQSQueue(queueUrl); // no more receiving requests from local applications
        EC2Methods.log.debug("deleted tasks queue");
        EC2Methods.log.debug("deleted the queue that local apps put their tasks");

        ec2Methods.terminateWorkers(totalWorkers);
        EC2Methods.log.debug("terminated workers");
        gotTerminate.set(true);
        try {
            System.out.println("Waiting for the final thread to finish the output files.");
            EC2Methods.log.debug("Waiting for the final thread to finish the output files.");
            buildFinalFileThread.join();
            System.out.println("The thread is done with the output files.");
            EC2Methods.log.debug("The thread is done with the output files.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        sqsMethods.deleteSQSQueue(finishedTasksQueueURL); // no more processing finished tasks
        EC2Methods.log.debug("deleted finished tasks queue");
        ec2Methods.terminateManager();
    }
}
