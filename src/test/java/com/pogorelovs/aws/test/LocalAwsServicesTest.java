package com.pogorelovs.aws.test;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.util.Arrays;

public class LocalAwsServicesTest {

    @Rule
    public final LocalStackContainer localstackContainer = new LocalStackContainer()
            .withServices(LocalStackContainer.Service.S3);

    /**
     * DocumentDB is compatible with mongo 3.6
     * Details: https://docs.aws.amazon.com/documentdb/latest/developerguide/functional-differences.html
     */
    @Rule
    public final MongoDBContainer documentDBContainer = new MongoDBContainer("mongo:3.6.0");


    @Test
    public void testS3FromLocalStack() {
        AmazonS3 s3Client = AmazonS3Client.builder()
                .withEndpointConfiguration(localstackContainer.getEndpointConfiguration(LocalStackContainer.Service.S3))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(
                        localstackContainer.getAccessKey(),
                        localstackContainer.getSecretKey()))
                )
                .build();

        System.out.println(s3Client.listBuckets());

        var bucketName = "test-bucket";
        Assertions.assertFalse(s3Client.doesBucketExistV2(bucketName));
        s3Client.createBucket(bucketName);
        Assertions.assertTrue(s3Client.doesBucketExistV2(bucketName));
    }

    @Test
    public void testDocumentDB() {
        MongoClient mongoClient = new MongoClient(documentDBContainer.getHost(), documentDBContainer.getFirstMappedPort());

        MongoDatabase testdb = mongoClient.getDatabase("test-db");
        MongoCollection<Document> dataCollection = testdb.getCollection("data");

        Assertions.assertEquals(0, dataCollection.countDocuments());

        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("versions", Arrays.asList("v3.2", "v3.0", "v2.6"))
                .append("info", new Document("x", 203).append("y", 102));
        dataCollection.insertOne(doc);

        Assertions.assertEquals(1, dataCollection.countDocuments());
    }
}
