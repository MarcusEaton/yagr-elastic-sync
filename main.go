package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ChangeEvent represents a MongoDB change stream event.
// It contains metadata about the operation and the document that was modified.
type ChangeEvent struct {
	// OperationType indicates the type of change (e.g., "insert", "update", "delete", "replace").
	OperationType string `bson:"operationType"`

	// DocumentKey holds the unique identifier of the modified document.
	DocumentKey struct {
		ID interface{} `bson:"_id"`
	} `bson:"documentKey"`

	// FullDocument contains the state of the document after the operation.
	// Note: For "delete" operations, this will typically be empty.
	FullDocument map[string]interface{} `bson:"fullDocument"`
}

func main() {
	// --- Configuration Setup ---
	// Load environment variables with fallback defaults for local development.
	mongoURI := os.Getenv("MONGO_URI")
	mongoDB := os.Getenv("MONGO_DB")
	mongoColl := os.Getenv("MONGO_COLLECTION")
	elasticURL := os.Getenv("ES_URL")
	elasticIndex := os.Getenv("MONGO_COLLECTION")
	elasticAPIKey := os.Getenv("ELASTIC_API_KEY")

	doInitialSync := getEnv("INITIAL_SYNC", "true") == "true"

	// Create a context that can be canceled for graceful application shutdown.
	ctx, cancel := context.WithCancel(context.Background())

	// --- Graceful Shutdown Handler ---
	// Listen for OS interrupt signals (e.g., Ctrl+C) to safely close connections before exiting.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Received termination signal, shutting down gracefully...")
		cancel()
	}()

	// 1. Connect to MongoDB
	log.Println("Connecting to MongoDB...")
	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("Error connecting to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(context.Background())

	if err := mongoClient.Ping(ctx, nil); err != nil {
		log.Fatalf("Error pinging MongoDB: %v", err)
	}

	collection := mongoClient.Database(mongoDB).Collection(mongoColl)

	// 2. Connect to Elasticsearch
	log.Println("Connecting to Elasticsearch...")
	esConfig := elasticsearch.Config{
		Addresses: []string{elasticURL},
		APIKey:    elasticAPIKey,
	}
	esClient, err := elasticsearch.NewClient(esConfig)
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %v", err)
	}
	info, err := esClient.Info()
	if err != nil {
		log.Fatalf("Error pinging Elasticsearch: %v", err)
	}
	info.Body.Close()

	// 3. Initial sync (Optional)
	if doInitialSync {
		log.Println("Performing initial sync...")
		performInitialSync(ctx, collection, esClient, elasticIndex)
	}

	// 4. Start MongoDB Change Stream
	log.Printf("Listening for live changes on Mongo (%s.%s)...", mongoDB, mongoColl)

	// Configure the change stream to include the full document for 'update' operations.
	// Without UpdateLookup, 'update' operations only include the delta of changed fields,
	// which prevents us from syncing the complete updated document to Elasticsearch.
	streamOptions := options.ChangeStream().SetFullDocument(options.UpdateLookup)

	// Open the change stream on the target collection. MongoDB must be configured as a replica set.
	changeStream, err := collection.Watch(ctx, mongo.Pipeline{}, streamOptions)
	if err != nil {
		log.Fatalf("Error opening mongo change stream (make sure your MongoDB is a replica set!): %v", err)
	}
	defer changeStream.Close(context.Background())

	log.Println("Real-time sync started. Press Ctrl+C to stop.")

	// Continuously iterate over change events as they arrive in real-time.
	for changeStream.Next(ctx) {
		var event ChangeEvent
		if err := changeStream.Decode(&event); err != nil {
			log.Printf("Error decoding change event: %v", err)
			continue
		}

		docID := parseID(event.DocumentKey.ID)

		switch event.OperationType {
		case "insert", "update", "replace":
			// For modifications or new documents, we push the state to Elasticsearch.
			doc := event.FullDocument
			if doc == nil {
				log.Printf("Warning: received %s for ID %s but fullDocument is empty. Ensure UpdateLookup is supported.", event.OperationType, docID)
				continue
			}

			// Remove the MongoDB '_id' field before sending to ES.
			// Elasticsearch reserves '_id' strictly for document metadata and throws an error
			// if it exists inside the JSON body.
			delete(doc, "_id")

			body, err := json.Marshal(doc)
			if err != nil {
				log.Printf("Error marshaling document ID %s: %v", docID, err)
				continue
			}

			// Create an Elasticsearch index request to insert or update the document.
			req := esapi.IndexRequest{
				Index:      elasticIndex,
				DocumentID: docID,
				Body:       strings.NewReader(string(body)),
				Refresh:    "true", // Refresh makes the document immediately searchable
			}
			res, err := req.Do(ctx, esClient)
			if err != nil {
				log.Printf("Error indexing document ID %s: %v", docID, err)
			} else {
				if res.IsError() {
					log.Printf("Elasticsearch error indexing %s: %s", docID, res.String())
				} else {
					log.Printf("Successfully synced %s (Type: %s)", docID, event.OperationType)
				}
				res.Body.Close()
			}

		case "delete":
			req := esapi.DeleteRequest{
				Index:      elasticIndex,
				DocumentID: docID,
				Refresh:    "true",
			}
			res, err := req.Do(ctx, esClient)
			if err != nil {
				log.Printf("Error deleting document ID %s: %v", docID, err)
			} else {
				if res.IsError() {
					log.Printf("Elasticsearch error deleting %s: %s", docID, res.String())
				} else {
					log.Printf("Successfully deleted %s", docID)
				}
				res.Body.Close()
			}

		default:
			log.Printf("Ignored operation type: %s", event.OperationType)
		}
	}

	if err := changeStream.Err(); err != nil {
		if err == context.Canceled {
			log.Println("Change stream closed due to cancellation.")
		} else {
			log.Printf("Change stream error: %v", err)
		}
	}
}

// performInitialSync wipes the target Elasticsearch index clean and does a full data dump
// from the MongoDB collection into Elasticsearch. This is useful for seeding data on startup.
func performInitialSync(ctx context.Context, collection *mongo.Collection, esClient *elasticsearch.Client, elasticIndex string) {
	// Step 1: Wipe the existing Elasticsearch index
	log.Printf("Clearing index '%s' using delete_by_query...", elasticIndex)
	delReq := esapi.DeleteByQueryRequest{
		Index: []string{elasticIndex},
		Body:  strings.NewReader(`{"query": {"match_all": {}}}`),
	}
	delRes, err := delReq.Do(ctx, esClient)
	if err != nil {
		log.Printf("Warning: failed to clear index limits: %v", err)
	} else {
		if delRes.IsError() {
			log.Printf("Warning: index clearing returned error (this is normal if the index doesn't exist yet): %s", delRes.String())
		} else {
			log.Println("Successfully cleared existing documents in the index.")
		}
		delRes.Body.Close()
	}

	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		log.Printf("Error querying MongoDB for initial sync: %v", err)
		return
	}
	defer cursor.Close(ctx)

	count := 0
	for cursor.Next(ctx) {
		var doc map[string]interface{}
		if err := cursor.Decode(&doc); err != nil {
			log.Printf("Error decoding MongoDB document: %v", err)
			continue
		}

		docID := ""
		if id, ok := doc["_id"]; ok {
			docID = parseID(id)
			delete(doc, "_id")
		}

		body, err := json.Marshal(doc)
		if err != nil {
			log.Printf("Error marshaling document to JSON: %v", err)
			continue
		}

		req := esapi.IndexRequest{
			Index:      elasticIndex,
			DocumentID: docID,
			Body:       strings.NewReader(string(body)),
		}

		res, err := req.Do(ctx, esClient)
		if err != nil {
			log.Printf("Error indexing document ID %s: %v", docID, err)
			continue
		}

		if res.IsError() {
			log.Printf("Elasticsearch error indexing document ID %s: %s", docID, res.String())
		}
		res.Body.Close()
		count++
		if count%100 == 0 {
			log.Printf("Initial sync: %d documents...", count)
		}
	}
	log.Printf("Initial sync finished! Successfully synced %d documents.", count)
}

// parseID safely extracts a string representation from a mixed-type MongoDB ID field.
// It converts `primitive.ObjectID` to its hex representation, and formats other types as strings.
func parseID(id interface{}) string {
	if oid, isOid := id.(primitive.ObjectID); isOid {
		return oid.Hex()
	}
	return fmt.Sprintf("%v", id)
}

// getEnv retrieves an environment variable's value by key.
// If the variable does not exist, it returns the provided fallback string.
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
