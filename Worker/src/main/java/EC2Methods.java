import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.StopInstancesRequest;

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

    public void stopInstance(String instanceId) {

        StopInstancesRequest request = StopInstancesRequest.builder()
                .instanceIds(instanceId)
                .build();

        ec2Client.stopInstances(request);
    }
}
