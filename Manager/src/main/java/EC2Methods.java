import org.apache.commons.codec.binary.Base64;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class EC2Methods {
    private final Ec2Client ec2Client;
    final public static String IAM_ROLE = "arn:aws:iam::935282201937:instance-profile/LabInstanceProfile";
    final public static String SECURITY_ID = "sg-03e1043c7ed636b1a";
    final public static String KEY_NAME = "dsps";

    private EC2Methods()
    {
        Region region = Region.US_EAST_1;
        this.ec2Client = Ec2Client.builder()
                .region(region)
                .build();
    }

    private static class Holder { // thread-safe singleton
        private static final EC2Methods INSTANCE = new EC2Methods();
    }

    public static EC2Methods getInstance() {
        return Holder.INSTANCE;
    }

    public void createEC2KeyPair(String keyName) {

        try {
            CreateKeyPairRequest request = CreateKeyPairRequest.builder()
                    .keyName(keyName).build();

            ec2Client.createKeyPair(request);
            System.out.printf(
                    "Successfully created key pair named %s",
                    keyName);

        } catch (Ec2Exception ignored) {}
    }

    public List<String> findWorkersByState(String state) {
        List<String> workersInstanceId = new LinkedList<>();
        try {
            String nextToken = null;

            do {
                Filter filter = Filter.builder()
                        .name("instance-state-name")
                        .values(state)
                        .build();

                DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                        .filters(filter)
                        .build();

                DescribeInstancesResponse response = ec2Client.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        System.out.printf(
                                "Found Reservation with id %s, " +
                                        "AMI %s, " +
                                        "type %s, " +
                                        "state %s \n",
                                instance.instanceId(),
                                instance.imageId(),
                                instance.instanceType(),
                                instance.state().name());
                        for(Tag tag : instance.tags())
                        {
                            if(tag.key().equals("W")) //found worker
                            {
                                System.out.println("Found worker instance! the state is: " + instance.state().name());
                                workersInstanceId.add(instance.instanceId());
                            }
                        }
                        System.out.println("");
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (Ec2Exception ec2Exception) {
            System.out.println(ec2Exception.awsErrorDetails());
        }

        return workersInstanceId;
    }

    public List<String> findWorkersByState(String state, List<String> workersToExclude) {
        List<String> workersInstanceId = new LinkedList<>();
        try {
            String nextToken = null;

            do {
                Filter filter = Filter.builder()
                        .name("instance-state-name")
                        .values(state)
                        .build();

                DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                        .filters(filter)
                        .build();

                DescribeInstancesResponse response = ec2Client.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        System.out.printf(
                                "Found Reservation with id %s, " +
                                        "AMI %s, " +
                                        "type %s, " +
                                        "state %s \n",
                                instance.instanceId(),
                                instance.imageId(),
                                instance.instanceType(),
                                instance.state().name());
                        for(Tag tag : instance.tags())
                        {
                            if(tag.key().equals("W") && !workersToExclude.contains(instance.instanceId())) //found worker
                            {
                                System.out.println("Found worker instance! the state is: " + instance.state().name());
                                workersInstanceId.add(instance.instanceId());
                            }
                        }
                        System.out.println("");
                    }
                }
                nextToken = response.nextToken();
            } while (nextToken != null);
        } catch (Ec2Exception ec2Exception) {
            System.out.println(ec2Exception.awsErrorDetails());
        }

        return workersInstanceId;
    }

    public void terminateWorkers(int totalWorkers) {
        try{
            System.out.println("total workers: " + totalWorkers);
            List<String> workersToExclude = new ArrayList<>();
            while(totalWorkers != 0)
            {
                List<String> workersInstanceId = findWorkersByState("stopped", workersToExclude);
                workersToExclude.addAll(workersInstanceId);
                if(workersInstanceId.isEmpty())
                    continue;
                System.out.println("Found workers to terminate: " + workersInstanceId);
                TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                        .instanceIds(workersInstanceId)
                        .build();

                TerminateInstancesResponse response = ec2Client.terminateInstances(ti);
                List<InstanceStateChange> list = response.terminatingInstances();
                System.out.println("Terminated " + list.size() + " workers");
                totalWorkers -= list.size();
                System.out.println("total workers after sub is: " + totalWorkers);

                for (InstanceStateChange sc : list) {
                    System.out.println("The ID of the terminated worker instance is " + sc.instanceId());
                }
            }
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails());
        }
    }

    public String findRunningManagerEC2Instances() {

        try {
            String nextToken;
            do {
                Filter filter = Filter.builder()
                        .name("instance-state-name")
                        .values("running")
                        .build();

                DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                        .filters(filter)
                        .build();

                DescribeInstancesResponse response = ec2Client.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        System.out.printf(
                                "Found Reservation with id %s, " +
                                        "AMI %s, " +
                                        "type %s, " +
                                        "state %s \n",
                                instance.instanceId(),
                                instance.imageId(),
                                instance.instanceType(),
                                instance.state().name());
                        for(Tag tag : instance.tags())
                        {
                            if(tag.key().equals("M")) //found manager
                            {
                                System.out.println("Found manager instance!");
                                return instance.instanceId();
                            }
                        }
                        System.out.println("");
                    }
                }
                nextToken = response.nextToken();

            } while (nextToken != null);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails());
        }
        return "";
    }

    public void terminateManager() {
        try{
            String managerInstanceId = findRunningManagerEC2Instances();
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(managerInstanceId)
                    .build();

            TerminateInstancesResponse response = ec2Client.terminateInstances(ti);
            InstanceStateChange managerDown = response.terminatingInstances().get(0);

            System.out.println("The ID of the terminated worker instance is " + managerDown.instanceId());
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails());
        }
    }

    public void createAndStartEC2WorkerInstance(String name, String amiId, int maxCount) {

        createEC2KeyPair(KEY_NAME);
        String userData = """
                #!/bin/sh
                rpm --import https://yum.corretto.aws/corretto.key
                curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo
                yum install -y java-15-amazon-corretto-devel
                aws s3 cp s3://workermanagerjarsbucket/Worker.jar .
                cd /
                java -jar Worker.jar""";

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(maxCount)
                .minCount(1)
                .securityGroupIds(SECURITY_ID)
                .keyName(KEY_NAME)
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn(IAM_ROLE).build())
                .userData(Base64.encodeBase64String(userData.getBytes(StandardCharsets.UTF_8)))
                .build();

        RunInstancesResponse response = ec2Client.runInstances(runRequest);
        List<Instance> instances = response.instances();

        Tag tag = Tag.builder()
                .key("W") //for worker
                .value(name)
                .build();

        for(Instance instance: instances)
        {
            String instanceId = instance.instanceId();
            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();
            try {
                ec2Client.createTags(tagRequest);
                System.out.printf(
                        "Successfully started EC2 Worker Instance %s based on AMI %s\n",
                        instanceId, amiId);
            } catch (Ec2Exception e) {
                System.err.println(e.awsErrorDetails().errorMessage());
            }

            startInstance(instance.instanceId());
        }
    }

    public void startInstance(String instanceId) {

        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2Client.startInstances(request);
        System.out.printf("Successfully started instance %s\n", instanceId);
    }
}
