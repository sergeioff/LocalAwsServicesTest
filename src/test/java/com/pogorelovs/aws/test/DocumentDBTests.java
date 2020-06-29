package com.pogorelovs.aws.test;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class DocumentDBTests {
    private final Logger logger = LoggerFactory.getLogger(DocumentDBTests.class);

    /**
     * DocumentDB is compatible with mongo 3.6
     * Details: https://docs.aws.amazon.com/documentdb/latest/developerguide/functional-differences.html
     */
    @Rule
    public final MongoDBContainer documentDBContainer = new MongoDBContainer("mongo:3.6.0");

    @Test
    public void testDbInsertion() {
        final MongoClient mongoClient = new MongoClient(documentDBContainer.getHost(), documentDBContainer.getFirstMappedPort());
        final MongoDatabase testDatabase = mongoClient.getDatabase("test-db");

        try {
            Files.list(Paths.get("db-init")).forEach(collectionPath -> {
                String collectionName = collectionPath.getFileName().toString();
                MongoCollection<Document> collection = testDatabase.getCollection(collectionName);
                try {
                    List<String> lines = Files.lines(collectionPath).collect(Collectors.toUnmodifiableList());

                    lines.stream()
                            .map(Document::parse)
                            .forEach(collection::insertOne);

                    Assertions.assertEquals(lines.size(), collection.countDocuments());
                } catch (IOException e) {
                    logger.error("Failed to read from file: ", e.getCause());
                }

                logger.info("Number of documents in {} : {}", collectionName, collection.countDocuments());


            });
        } catch (IOException e) {
            logger.error("Failed to access file: ", e.getCause());
        }
    }
}
