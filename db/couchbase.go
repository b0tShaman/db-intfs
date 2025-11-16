package db

import (
	"fmt"
	"time"

	gocb "github.com/couchbase/gocb/v2"
	log "github.com/sirupsen/logrus"
)

const (
	DelayBetweenConnectTries = 5 // seconds
	OpGet                    = "GET"
	OpInsert                 = "INSERT"
	OpUpsert                 = "UPSERT"
	OpRemove                 = "REMOVE"
	OpCounter                = "COUNTER"
	OpTouch                  = "TOUCH"
)

type OpFunc func() OpResult

type CouchbaseClient struct {
	cluster    *gocb.Cluster
	bucket     *gocb.Bucket
	collection *gocb.Collection
}

type Op struct {
	Key string
	Do  OpFunc
}

type OpResult struct {
	Key  string
	Type string
	Err  error
}

type ICouchbase interface {
	// Basic CRUD operations
	Get(key string, valuePtr any) (gocb.Cas, error)
	Insert(key string, value any, expiry uint32) (gocb.Cas, error)
	Upsert(key string, value any, expiry uint32) (gocb.Cas, error)
	Remove(key string, cas gocb.Cas) (gocb.Cas, error)

	// Counter operations
	Counter(key string, delta, initial int64, expiry uint32) (uint64, gocb.Cas, error)

	// Subdocument operations
	MutateIn(key string, cas gocb.Cas, mutateOps []gocb.MutateInSpec, expiry uint32) (*gocb.MutateInResult, error)

	// Bulk operations
	Do(ops []Op) error
	GetOp(key string, target any) Op
	InsertOp(key string, value any, expiry uint32) Op
	UpsertOp(key string, value any, expiry uint32) Op
	RemoveOp(key string, cas gocb.Cas) Op
	CounterOp(key string, delta, initial int64, expiry uint32) Op
	TouchOp(key string, expiry uint32) Op
}

func (cb *CouchbaseClient) Get(key string, valuePtr any) (gocb.Cas, error) {
	res, err := cb.collection.Get(key, &gocb.GetOptions{})
	if err == nil {
		err = res.Content(valuePtr)
		if err != nil {
			log.Error(fmt.Sprintf("Get: Failed to read content for key:%s Err: %v", key, err))
		}
		return res.Cas(), err
	}
	return 0, err
}

func (cb *CouchbaseClient) Insert(key string, value any, expiry uint32) (gocb.Cas, error) {
	res, err := cb.collection.Insert(key, value, &gocb.InsertOptions{Expiry: expiryFromSeconds(expiry)})
	if err == nil {
		return res.Cas(), nil
	}
	return 0, err
}

func (cb *CouchbaseClient) Upsert(key string, value any, expiry uint32) (gocb.Cas, error) {
	res, err := cb.collection.Upsert(key, value, &gocb.UpsertOptions{Expiry: expiryFromSeconds(expiry)})
	if err == nil {
		return res.Cas(), nil
	}
	return 0, err
}

func (cb *CouchbaseClient) Remove(key string, cas gocb.Cas) (gocb.Cas, error) {
	res, err := cb.collection.Remove(key, &gocb.RemoveOptions{Cas: cas})
	if err == nil {
		return res.Cas(), nil
	}
	return 0, err
}

func (cb *CouchbaseClient) Counter(key string, delta, initial int64, expiry uint32) (uint64, gocb.Cas, error) {
	res, err := cb.collection.Binary().Increment(key, &gocb.IncrementOptions{Delta: uint64(delta), Initial: initial, Expiry: expiryFromSeconds(expiry)})
	if err == nil {
		return res.Content(), res.Cas(), nil
	}
	return 0, 0, err
}

func (cb *CouchbaseClient) MutateIn(key string, cas gocb.Cas, mutateOps []gocb.MutateInSpec, expiry uint32) (*gocb.MutateInResult, error) {
	res, err := cb.collection.MutateIn(key, mutateOps, &gocb.MutateInOptions{Cas: cas, Expiry: expiryFromSeconds(expiry)})
	if err == nil {
		return res, nil
	}
	return nil, err
}

// Returns last error encountered, if any
func (cb *CouchbaseClient) Do(ops []Op) error {
	var lastErr error
	results := make(chan OpResult, len(ops))
	for _, op := range ops {
		go func(op Op) {
			results <- op.Do()
		}(op)
	}

	for range ops {
		res := <-results
		if res.Err != nil {
			log.Error(fmt.Sprintf("Operation %s failed for key %s: %v", res.Type, res.Key, res.Err))
			lastErr = res.Err
		}
	}
	return lastErr
}

func (cb *CouchbaseClient) GetOp(key string, target any) Op {
	return Op{
		Key: key,
		Do: func() OpResult {
			res, err := cb.collection.Get(key, &gocb.GetOptions{})
			if err == nil {
				err = res.Content(target)
				if err != nil {
					log.Error(fmt.Sprintf("GetOp: Failed to read content for key:%s Err: %v", key, err))
				}
			}
			return OpResult{Key: key, Type: OpGet, Err: err}
		},
	}
}

func (cb *CouchbaseClient) InsertOp(key string, value any, expiry uint32) Op {
	return Op{
		Key: key,
		Do: func() OpResult {
			_, err := cb.collection.Insert(key, value, &gocb.InsertOptions{Expiry: expiryFromSeconds(expiry)})
			return OpResult{Key: key, Type: OpInsert, Err: err}
		},
	}
}

func (cb *CouchbaseClient) UpsertOp(key string, value any, expiry uint32) Op {
	return Op{
		Key: key,
		Do: func() OpResult {
			_, err := cb.collection.Upsert(key, value, &gocb.UpsertOptions{Expiry: expiryFromSeconds(expiry)})
			return OpResult{Key: key, Type: OpUpsert, Err: err}
		},
	}
}

func (cb *CouchbaseClient) RemoveOp(key string, cas gocb.Cas) Op {
	return Op{
		Key: key,
		Do: func() OpResult {
			_, err := cb.collection.Remove(key, &gocb.RemoveOptions{Cas: cas})
			return OpResult{Key: key, Type: OpRemove, Err: err}
		},
	}
}

func (cb *CouchbaseClient) CounterOp(key string, delta, initial int64, expiry uint32) Op {
	return Op{
		Key: key,
		Do: func() OpResult {
			_, err := cb.collection.Binary().Increment(key, &gocb.IncrementOptions{Delta: uint64(delta), Initial: initial, Expiry: expiryFromSeconds(expiry)})
			return OpResult{Key: key, Type: OpCounter, Err: err}
		},
	}
}

func (cb *CouchbaseClient) TouchOp(key string, expiry uint32) Op {
	return Op{
		Key: key,
		Do: func() OpResult {
			_, err := cb.collection.Touch(key, expiryFromSeconds(expiry), &gocb.TouchOptions{})
			return OpResult{Key: key, Type: OpTouch, Err: err}
		},
	}
}

// InitCouchbase connects to cluster, opens bucket and default collection
func NewCouchbaseClient(couchbaseURL, username, password, bucketName, scopeName, collectionName string) *CouchbaseClient {
	var cluster *gocb.Cluster
	var bucket *gocb.Bucket
	var err error
	var collection *gocb.Collection
	log.Info("Connecting to Couchbase at " + couchbaseURL)
	// Retry cluster connect until success
	for {
		cluster, err = gocb.Connect(couchbaseURL, gocb.ClusterOptions{
			Username: username,
			Password: password,
		})
		if err == nil {
			// Wait for cluster to be ready
			if werr := cluster.WaitUntilReady(10*time.Second, nil); werr == nil {
				log.Info("Connected to Couchbase cluster")
				break
			} else {
				log.Warn(fmt.Sprintf("Cluster not ready: %v", werr))
			}
		} else {
			log.Error(fmt.Sprintf("Error connecting to Couchbase cluster: %v", err))
		}
		time.Sleep(DelayBetweenConnectTries * time.Second)
	}

	// Retry bucket open until success
	bucket = cluster.Bucket(bucketName)
	for {
		err = bucket.WaitUntilReady(10*time.Second, nil)
		if err == nil {
			log.Info(fmt.Sprintf("Bucket [%s] is ready", bucketName))
			break
		}
		log.Error(fmt.Sprintf("Error opening bucket [%s]: %v", bucketName, err))
		time.Sleep(DelayBetweenConnectTries * time.Second)
	}

	// Get collection
	if scopeName != "" && collectionName != "" {
		collection = bucket.Scope(scopeName).Collection(collectionName)
		log.Info(fmt.Sprintf("Using scope [%s], collection [%s]", scopeName, collectionName))
	} else {
		collection = bucket.DefaultCollection()
		log.Info(fmt.Sprintf("Using default collection from bucket [%s]", bucketName))
	}

	return &CouchbaseClient{
		cluster:    cluster,
		bucket:     bucket,
		collection: collection,
	}
}

// Shutdown gracefully closes the cluster
func (cb *CouchbaseClient) Shutdown() {
	if cb.cluster != nil {
		err := cb.cluster.Close(nil)
		if err != nil {
			log.Error(fmt.Sprintf("Error closing Couchbase cluster: %v", err))
		} else {
			log.Info("Couchbase cluster connection closed")
		}
	}
}

func expiryFromSeconds[T ~int | ~uint32](secs T) time.Duration {
	return time.Duration(secs) * time.Second
}