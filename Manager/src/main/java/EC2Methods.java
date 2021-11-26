import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.LinkedList;
import java.util.List;

public class EC2Methods {
    private final Ec2Client ec2Client;

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

    public List<String> findRunningWorkersEC2Instances() {
        List<String> workersInstanceId = new LinkedList<>();
        try {
            String nextToken = null;

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
                            if(tag.key().equals("W")) //found worker
                            {
                                System.out.println("Found worker instance!");
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

    public void terminateWorkers() {
        try{
            List<String> workersInstanceId = findRunningWorkersEC2Instances();
            TerminateInstancesRequest ti = TerminateInstancesRequest.builder()
                    .instanceIds(workersInstanceId)
                    .build();

            TerminateInstancesResponse response = ec2Client.terminateInstances(ti);
            List<InstanceStateChange> list = response.terminatingInstances();

            for (InstanceStateChange sc : list) {
                System.out.println("The ID of the terminated worker instance is " + sc.instanceId());
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

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(maxCount)
                .minCount(1)
                /*.userData("#!/bin/sh\n" +
                        "sudo yum install java-1.8.0-openjdk\n" +
                        "aws s3 cp s3://bucket/folder/manager.jar .\n" +
                        "java -jar /home/ubuntu/manager.jar")*/
                .build();

        RunInstancesResponse response = ec2Client.runInstances(runRequest);
        List<Instance> instances = response.instances();

//        Tag tag = Tag.builder()
//                .key("W") //for worker
//                .value(name)
//                .build();

        for(Instance instance: instances)
        {
//            String instanceId = instance.instanceId();
//            CreateTagsRequest tagRequest = CreateTagsRequest.builder()
//                    .resources(instanceId)
//                    .tags(tag)
//                    .build();
//            try {
//                ec2.createTags(tagRequest);
//                System.out.printf(
//                        "Successfully started EC2 Worker Instance %s based on AMI %s\n",
//                        instanceId, amiId);
//
//            } catch (Ec2Exception e) {
//                System.err.println(e.awsErrorDetails().errorMessage());
//                System.exit(1);
//            }

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
