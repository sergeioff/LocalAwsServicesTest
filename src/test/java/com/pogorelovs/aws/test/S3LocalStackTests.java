package com.pogorelovs.aws.test;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.localstack.LocalStackContainer;

public class S3LocalStackTests {

    @Rule
    public final LocalStackContainer localstackContainer = new LocalStackContainer()
            .withServices(LocalStackContainer.Service.S3);

    @Test
    public void testS3WithLocalStack() {
        AmazonS3 s3Client = AmazonS3Client.builder()
                .withEndpointConfiguration(localstackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        localstackContainer.getAccessKey(),
                        localstackContainer.getSecretKey()))
                )
                .build();

        var bucketName = "test-bucket";
        Assertions.assertFalse(s3Client.doesBucketExistV2(bucketName));
        s3Client.createBucket(bucketName);
        Assertions.assertTrue(s3Client.doesBucketExistV2(bucketName));
    }
}
