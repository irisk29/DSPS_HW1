import org.apache.commons.codec.binary.Base64;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.nio.charset.StandardCharsets;

public class EC2Methods {
    private final Ec2Client ec2Client;
    final private String IAM_ROLE = "arn:aws:iam::145327589717:instance-profile/LabInstanceProfile";
    final private String SECURITY_ID = "sg-094e2c3380c267626";
    final private String KEY_NAME = "dsps";

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

    private void createEC2KeyPair(String keyName) {
        try {
            CreateKeyPairRequest request = CreateKeyPairRequest.builder()
                    .keyName(keyName).build();
            this.ec2Client.createKeyPair(request);
            System.out.printf("Successfully created key pair named %s", keyName);
        } catch (Ec2Exception ignored) {}
    }

    private void createEC2Tag(String instanceId, String tagKey, String tagValue) {
        Tag tag = Tag.builder()
                .key(tagKey)
                .value(tagValue)
                .build();
        CreateTagsRequest tagRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();
        this.ec2Client.createTags(tagRequest);
        System.out.printf("Successfully started EC2 Instance %s\n", instanceId);
    }

    public String createEC2Instance(String tagKey, String tagValue, String amiId, InstanceType instanceType) {
        try {
            createEC2KeyPair(KEY_NAME);

            String userData = """
                    #!/bin/sh
                    rpm --import https://yum.corretto.aws/corretto.key
                    curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo
                    yum install -y java-15-amazon-corretto-devel
                    aws s3 cp s3://workermanagerjarsbucket/Manager.jar .
                    cd /
                    java -jar Manager.jar""";
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(amiId)
                    .instanceType(instanceType)
                    .maxCount(1)
                    .minCount(1)
                    .securityGroupIds(SECURITY_ID)
                    .keyName(KEY_NAME)
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().arn(IAM_ROLE).build())
                    .userData(Base64.encodeBase64String(userData.getBytes(StandardCharsets.UTF_8)))
                    .build();
            RunInstancesResponse response = this.ec2Client.runInstances(runRequest);
            String instanceId = response.instances().get(0).instanceId();

            createEC2Tag(instanceId, tagKey, tagValue);

            return instanceId;
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return null;
        }
    }

    public void startInstance(String instanceId) {
        StartInstancesRequest request = StartInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();
        this.ec2Client.startInstances(request);
        System.out.printf("Successfully started instance %s\n", instanceId);
    }

    public boolean isEC2InstanceRunning(String tagKey) {
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
                DescribeInstancesResponse response = this.ec2Client.describeInstances(request);

                for (Reservation reservation : response.reservations()) {
                    for (Instance instance : reservation.instances()) {
                        for(Tag tag : instance.tags())
                        {
                            if(tag.key().equals(tagKey))
                            {
                                System.out.println("Found " + tagKey + " instance!");
                                return true;
                            }
                        }
                    }
                }
                nextToken = response.nextToken();

            } while (nextToken != null);
            return false;
        } catch (Ec2Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return false;
        }
    }
}
