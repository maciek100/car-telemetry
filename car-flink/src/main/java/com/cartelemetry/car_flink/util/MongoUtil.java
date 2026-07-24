package com.cartelemetry.car_flink.util;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class MongoUtil {
    private static final String DB_NAME = "cartelemetry";
    private static MongoClient client;

    private MongoUtil () {
        throw new UnsupportedOperationException("MongoUtil is a utility class");
    }
    // Singleton — one client shared across all functions
    private static synchronized MongoClient getClient() {
        if (client == null) {
            String mongoUri = System.getenv().getOrDefault(
                    "MONGODB_URI", "mongodb://localhost:27017");
            client = MongoClients.create(mongoUri);
        }
        return client;
    }

    private static MongoDatabase getDatabase() {
        return getClient().getDatabase(DB_NAME);
    }

    public static MongoCollection<Document> getCollection(
            String collectionName) {
        return getDatabase().getCollection(collectionName);
    }

    public static void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

}
