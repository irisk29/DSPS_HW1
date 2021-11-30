import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

import java.io.*;
import java.net.URL;

public class S3Methods {
    private final S3Client s3Client;

    private S3Methods() {
        Region region = Region.US_EAST_1;
        this.s3Client = S3Client.builder()
                .region(region)
                .build();
    }

    private static class Holder { // thread-safe singleton
        private static final S3Methods INSTANCE = new S3Methods();
    }

    public static S3Methods getInstance() {
        return S3Methods.Holder.INSTANCE;
    }

    public void createBucket(String bucketName) {
        try {
            S3Waiter s3Waiter = this.s3Client.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            this.s3Client.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            // Wait until the bucket is created and print out the response.
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println(bucketName +" is ready");
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    // checks if the bucketName already been created
    public boolean isBucketExist(String bucketName) {
        try {
            HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();
            this.s3Client.headBucket(headBucketRequest);
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        }
    }

    // Return a byte array
    private byte[] getObjectFile(String filePath) {
        FileInputStream fileInputStream = null;
        byte[] bytesArray = null;

        try {
            File file = new File(filePath);
            bytesArray = new byte[(int) file.length()];
            fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytesArray);

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bytesArray;
    }

    public void putS3Object(String bucketName, String objectKey, String objectPath) {
        try {
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build();
            this.s3Client.putObject(putOb, RequestBody.fromBytes(getObjectFile(objectPath)));
        } catch (S3Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public File getS3ObjectAsFile(String bucketName, String keyName, String outputFileName) {
        try {
            GetObjectRequest objectRequest = GetObjectRequest
                    .builder()
                    .key(keyName)
                    .bucket(bucketName)
                    .build();
            ResponseBytes<GetObjectResponse> objectBytes = this.s3Client.getObjectAsBytes(objectRequest);

            byte[] data = objectBytes.asByteArray();
            File myFile = new File(outputFileName); // Write the data to a local file
            OutputStream os = new FileOutputStream(myFile);
            os.write(data);
            System.out.println("Successfully obtained bytes from an S3 object");
            os.close();
            return myFile;
        } catch (Exception e) {
            System.err.println(e);
            return null;
        }
    }

    public URL getS3ObjectURL(String bucketName, String keyName) {
        try {
            GetUrlRequest request = GetUrlRequest.builder()
                    .bucket(bucketName)
                    .key(keyName)
                    .build();
            return this.s3Client.utilities().getUrl(request);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            return null;
        }
    }

    public void deleteS3Object(String bucketName, String objectKey) {
        try {
            this.s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .build());
        } catch (S3Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public void deleteBucket(String bucketName) {
        try {
            // To delete a bucket, all the objects in the bucket must be deleted first
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName).build();
            ListObjectsV2Response listObjectsV2Response;
            do {
                listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    deleteS3Object(bucketName, s3Object.key());
                }
                listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketName)
                        .continuationToken(listObjectsV2Response.nextContinuationToken())
                        .build();
            } while(listObjectsV2Response.isTruncated());

            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucketName).build();
            this.s3Client.deleteBucket(deleteBucketRequest);
        } catch (S3Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
