package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/RedisLabs/redisearch-go/redisearch"
	"github.com/rwynn/gtm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"
)

type indexStats struct {
	Total      int64
	Indexed    int64
	Failed     int64
	Flushed    int64
	sync.Mutex `json:"-"`
}

type loggers struct {
	infoLog  *log.Logger
	warnLog  *log.Logger
	statsLog *log.Logger
	traceLog *log.Logger
	errorLog *log.Logger
}

type indexBuffer struct {
	worker        *indexWorker
	items         []redisearch.Document
	maxDuration   time.Duration
	maxItems      int
	maxSize       int64
	stopC         chan bool
	maxRetry      int
	retryDuration time.Duration
	logs          *loggers
	stats         *indexStats
	createIndex   bool
	curSize       int64
}

type indexWorker struct {
	client      *redisearch.Client
	namespace   string
	workQ       chan *gtm.Op
	buffers     int
	maxDuration time.Duration
	maxItems    int
	maxSize     int64
	stopC       chan bool
	allWg       *sync.WaitGroup
	logs        *loggers
	stats       *indexStats
	createIndex bool
}

type indexClient struct {
	readContext   *gtm.OpCtx
	addrs         string
	redisClients  map[string]*redisearch.Client
	workers       map[string]*indexWorker
	stopC         chan bool
	maxDuration   time.Duration
	maxItems      int
	maxSize       int64
	allWg         *sync.WaitGroup
	logs          *loggers
	stats         *indexStats
	createIndex   bool
	statsDuration time.Duration
}

func opIDToString(op *gtm.Op) (id string) {
	switch val := op.Id.(type) {
	case primitive.ObjectID:
		id = val.Hex()
	case float64:
		intID := int(val)
		if op.Id.(float64) == float64(intID) {
			id = fmt.Sprintf("%v", intID)
		} else {
			id = fmt.Sprintf("%v", op.Id)
		}
	case float32:
		intID := int(val)
		if op.Id.(float32) == float32(intID) {
			id = fmt.Sprintf("%v", intID)
		} else {
			id = fmt.Sprintf("%v", op.Id)
		}
	default:
		id = fmt.Sprintf("%v", op.Id)
	}
	return
}

func (is *indexStats) dup() *indexStats {
	is.Lock()
	defer is.Unlock()
	return &indexStats{
		Total:   is.Total,
		Flushed: is.Flushed,
		Indexed: is.Indexed,
		Failed:  is.Failed,
	}
}

func (is *indexStats) addFlushed() {
	is.Lock()
	defer is.Unlock()
	is.Flushed++
}

func (is *indexStats) addTotal(count int) {
	is.Lock()
	defer is.Unlock()
	is.Total += int64(count)
}

func (is *indexStats) addIndexed(count int) {
	is.Lock()
	defer is.Unlock()
	is.Indexed += int64(count)
}

func (is *indexStats) addFailed(count int) {
	is.Lock()
	defer is.Unlock()
	is.Failed += int64(count)
}

func (ib *indexBuffer) createDoc(op *gtm.Op) redisearch.Document {
	docId := opIDToString(op)
	doc := redisearch.NewDocument(docId, 1.0)
	for k, v := range op.Data {
		if k == "_id" {
			continue
		}
		switch val := v.(type) {
		case map[string]interface{}:
			continue
		case []interface{}:
			continue
		case time.Time:
			doc.Set(k, val.Unix())
		default:
			doc.Set(k, val)
		}
	}
	return doc
}

func (ib *indexBuffer) toSchema() *redisearch.Schema {
	sc := redisearch.NewSchema(redisearch.DefaultOptions)
	if len(ib.items) > 0 {
		item := ib.items[0]
		if item.Properties != nil {
			for k, v := range item.Properties {
				switch v.(type) {
				case map[string]interface{}:
					break
				case []interface{}:
					break
				case time.Time:
					sc.AddField(redisearch.NewNumericField(k))
				case int:
					sc.AddField(redisearch.NewNumericField(k))
				case int32:
					sc.AddField(redisearch.NewNumericField(k))
				case int64:
					sc.AddField(redisearch.NewNumericField(k))
				case float32:
					sc.AddField(redisearch.NewNumericField(k))
				case float64:
					sc.AddField(redisearch.NewNumericField(k))
				default:
					sc.AddField(redisearch.NewTextField(k))
				}
			}
		}
	}
	return sc
}

func (ib *indexBuffer) afterFlush(err error) {
	ib.stats.addFlushed()
	ib.stats.addTotal(len(ib.items))
	if err == nil {
		ib.stats.addIndexed(len(ib.items))
		return
	}
	multiError, ok := err.(redisearch.MultiError)
	if ok {
		ib.stats.addFailed(len(multiError))
		ib.stats.addIndexed(len(ib.items) - len(multiError))
	} else {
		ib.stats.addFailed(len(ib.items))
	}
}

func (ib *indexBuffer) logIndexInfo(indexInfo *redisearch.IndexInfo) {
	if b, err := json.Marshal(indexInfo); err == nil {
		ib.logs.infoLog.Printf("Auto created index: %s", string(b))
	}
}

func (ib *indexBuffer) autoCreateIndex() (*redisearch.IndexInfo, error) {
	var indexInfo *redisearch.IndexInfo
	var err error
	schema := ib.toSchema()
	client := ib.worker.client
	if err = client.CreateIndex(schema); err == nil {
		indexInfo, err = client.Info()
	}
	return indexInfo, err
}

func (ib *indexBuffer) flush() (err error) {
	if len(ib.items) == 0 {
		ib.items = nil
		ib.curSize = 0
		return
	}
	docs := ib.items
	client := ib.worker.client
	indexOptions := redisearch.IndexingOptions{Replace: true}
	err = client.IndexOptions(indexOptions, docs...)
	if err != nil {
		_, err = client.Info()
		if err != nil && ib.createIndex {
			if indexInfo, cie := ib.autoCreateIndex(); cie == nil {
				ib.logIndexInfo(indexInfo)
			}
			err = client.IndexOptions(indexOptions, docs...)
		}
	}
	ib.afterFlush(err)
	ib.items = nil
	ib.curSize = 0
	return
}

func (ib *indexBuffer) full() bool {
	if ib.maxItems != 0 {
		if len(ib.items) >= ib.maxItems {
			return true
		}
	}
	if ib.maxSize != 0 {
		if ib.curSize >= ib.maxSize {
			return true
		}
	}
	return false
}

func (ib *indexBuffer) addItem(op *gtm.Op) {
	doc := ib.createDoc(op)
	ib.items = append(ib.items, doc)
	ib.curSize += int64(doc.EstimateSize())
	if ib.full() {
		if err := ib.flush(); err != nil {
			ib.logs.errorLog.Printf("Indexing failed: %s", err)
		}
	}
}

func (ib *indexBuffer) run() {
	timer := time.NewTicker(ib.maxDuration)
	defer timer.Stop()
	done := false
	for !done {
		select {
		case op := <-ib.worker.workQ:
			ib.addItem(op)
		case <-timer.C:
			if err := ib.flush(); err != nil {
				ib.logs.errorLog.Printf("Indexing failed: %s", err)
			}
		case <-ib.stopC:
			done = true
		}
	}
	if err := ib.flush(); err != nil {
		ib.logs.errorLog.Printf("Indexing failed: %s", err)
	}
}

func (iw *indexWorker) start() {
	for i := 0; i < iw.buffers; i++ {
		iw.allWg.Add(1)
		buf := &indexBuffer{
			worker:      iw,
			maxDuration: iw.maxDuration,
			maxItems:    iw.maxItems,
			maxSize:     iw.maxSize,
			stopC:       iw.stopC,
			logs:        iw.logs,
			stats:       iw.stats,
			createIndex: iw.createIndex,
		}
		go buf.run()
	}
}

func (ic *indexWorker) add(op *gtm.Op) *indexWorker {
	ic.workQ <- op
	return ic
}

func newLoggers() *loggers {
	return &loggers{
		infoLog:  log.New(os.Stdout, "INFO ", log.Flags()),
		warnLog:  log.New(os.Stdout, "WARN ", log.Flags()),
		statsLog: log.New(os.Stdout, "STATS ", log.Flags()),
		traceLog: log.New(os.Stdout, "TRACE ", log.Flags()),
		errorLog: log.New(os.Stderr, "ERROR ", log.Flags()),
	}
}

func newIndexClient(ctx *gtm.OpCtx) *indexClient {
	return &indexClient{
		readContext:   ctx,
		allWg:         &sync.WaitGroup{},
		stopC:         make(chan bool),
		addrs:         "localhost:6379",
		redisClients:  make(map[string]*redisearch.Client),
		workers:       make(map[string]*indexWorker),
		maxDuration:   time.Duration(1) * time.Second,
		maxItems:      1000,
		maxSize:       0,
		logs:          newLoggers(),
		stats:         &indexStats{},
		createIndex:   true,
		statsDuration: time.Duration(10) * time.Second,
	}
}

func (ic *indexClient) sigListen() {
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer signal.Stop(sigs)
	<-sigs
	go func() {
		<-sigs
		ic.logs.infoLog.Println("Forcing shutdown")
		os.Exit(1)
	}()
	ic.logs.infoLog.Println("Shutting down")
	ic.readContext.Stop()
	ic.stop()
	os.Exit(0)
}

func (ic *indexClient) logStats() {
	stats := ic.stats.dup()
	if b, err := json.Marshal(stats); err == nil {
		ic.logs.infoLog.Printf("Indexing stats: %s", string(b))
	}
}

func (ic *indexClient) statsLoop() {
	heartBeat := time.NewTicker(ic.statsDuration)
	defer heartBeat.Stop()
	done := false
	for !done {
		select {
		case <-heartBeat.C:
			ic.logStats()
		case <-ic.stopC:
			ic.logStats()
			done = true
		}
	}
}

func (client *indexClient) eventLoop() {
	go client.sigListen()
	go client.statsLoop()
	ctx := client.readContext
	for {
		select {
		case err := <-ctx.ErrC:
			if err == nil {
				break
			}
			client.logs.errorLog.Println(err)
		case op := <-ctx.OpC:
			if op == nil {
				break
			}
			client.add(op)
		}
	}
}

func (ic *indexClient) setAddrs(s string) *indexClient {
	ic.addrs = s
	return ic
}

func (ic *indexClient) setCreateIndex(b bool) *indexClient {
	ic.createIndex = b
	return ic
}

func (ic *indexClient) stop() {
	close(ic.stopC)
	ic.allWg.Wait()
}

func (ic *indexClient) add(op *gtm.Op) *indexClient {
	ns := op.Namespace
	c := ic.redisClients[ns]
	if c == nil {
		c = redisearch.NewClient(ic.addrs, ns)
		ic.redisClients[ns] = c
	}
	worker := ic.workers[ns]
	if worker == nil {
		worker = &indexWorker{
			client:      c,
			buffers:     4,
			namespace:   op.Namespace,
			maxDuration: ic.maxDuration,
			maxItems:    ic.maxItems,
			maxSize:     ic.maxSize,
			workQ:       make(chan *gtm.Op),
			stopC:       ic.stopC,
			allWg:       ic.allWg,
			logs:        ic.logs,
			stats:       ic.stats,
			createIndex: ic.createIndex,
		}
		ic.workers[ns] = worker
		worker.start()
	}
	worker.add(op)
	return ic
}

func buildRegistry() *bsoncodec.Registry {
	rb := bson.NewRegistryBuilder()
	rb.RegisterTypeMapEntry(bsontype.DateTime, reflect.TypeOf(time.Time{}))
	return rb.Build()
}

func dialMongo(URI string) (*mongo.Client, error) {
	clientOptions := options.Client()
	clientOptions.SetRegistry(buildRegistry())
	clientOptions.ApplyURI(URI)
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		return nil, err
	}
	ctxm, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err = client.Connect(ctxm); err != nil {
		return nil, err
	}
	return client, nil
}

func main() {
	var errorLog = log.New(os.Stderr, "ERROR ", log.Flags())
	client, err := dialMongo("mongodb://localhost:27017")
	if err != nil {
		errorLog.Fatalf("MongoDB connection failed: %s", err)
	}
	defer client.Disconnect(context.Background())
	ctx := gtm.Start(client, &gtm.Options{
		ChangeStreamNs: []string{""},
		OpLogDisabled:  true,
	})
	indexClient := newIndexClient(ctx)
	indexClient.eventLoop()
}
