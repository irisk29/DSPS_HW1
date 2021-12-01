import org.apache.commons.codec.binary.Base64;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class EC2Methods {
    private final Ec2Client ec2Client;
    public static String IAM_ROLE;
    public static String SECURITY_ID;
    public static String KEY_NAME;
    public static String AMI;

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
                        for(Tag tag : instance.tags())
                        {
                            if(tag.key().equals("W")) //found worker
                            {
                                workersInstanceId.add(instance.instanceId());
                            }
                        }
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
                        for(Tag tag : instance.tags())
                        {
                            if(tag.key().equals("W") && !workersToExclude.contains(instance.instanceId())) //found worker
                            {
                                workersInstanceId.add(instance.instanceId());
                            }
                        }
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
            List<String> workersToExclude = new ArrayList<>();
            while(totalWorkers != 0)
            {
                List<String> workersInstanceId = findWorkersByState("stopped", workersToExclude);
                workersToExclude.addAll(workersInstanceId);
                if(workersInstanceId.isEmpty())
                    continue;
                TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                        .instanceIds(workersInstanceId)
                        .build();

                TerminateInstancesResponse response = ec2Client.terminateInstances(ti);
                List<InstanceStateChange> list = response.terminatingInstances();
                totalWorkers -= list.size();
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
                        for(Tag tag : instance.tags())
                        {
                            if(tag.key().equals("M")) //found manager
                            {
                                return instance.instanceId();
                            }
                        }
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

            ec2Client.terminateInstances(ti);
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails());
        }
    }

    public void createAndStartEC2WorkerInstance(String name, int maxCount) {
        String userData = """
                #!/bin/sh
                rpm --import https://yum.corretto.aws/corretto.key
                curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo
                yum install -y java-15-amazon-corretto-devel
                aws s3 cp s3://workermanagerjarsbucket/Worker.jar .
                cd /
                java -jar Worker.jar""";

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(AMI)
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
    }
}
