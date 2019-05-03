package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/RediSearch/redisearch-go/redisearch"
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

const (
	version = "0.0.1"
)

var (
	infoLog  = log.New(os.Stderr, "INFO ", log.Flags())
	warnLog  = log.New(os.Stdout, "WARN ", log.Flags())
	statsLog = log.New(os.Stdout, "STATS ", log.Flags())
	traceLog = log.New(os.Stdout, "TRACE ", log.Flags())
	errorLog = log.New(os.Stderr, "ERROR ", log.Flags())
)

type stringargs []string

type config struct {
	ConfigFile           string
	MongoURI             string     `toml:"mongo"`
	RedisearchAddrs      string     `toml:"redisearch"`
	DisableCreateIndex   bool       `toml:"disable-create-index"`
	MaxItems             int        `toml:"max-items"`
	MaxSize              int64      `toml:"max-size"`
	MaxDuration          string     `toml:"max-duration"`
	StatsDuration        string     `toml:"stats-duration"`
	Indexers             int        `toml:"indexers"`
	ChangeStreamNs       stringargs `toml:"change-stream-namespaces"`
	DirectReadNs         stringargs `toml:"direct-read-namespaces"`
	DirectReadSplitMax   int        `toml:"direct-read-split-max"`
	DirectReadConcur     int        `toml:"direct-read-concur"`
	FailFast             bool       `toml:"fail-fast"`
	ExitAfterDirectReads bool       `toml:"exit-after-direct-reads"`
	DisableStats         bool       `toml:"disable-stats"`
}

func (c *config) hasFlag(name string) bool {
	passed := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			passed = true
		}
	})
	return passed
}

func (c *config) log(out *log.Logger) {
	if b, err := json.MarshalIndent(c, "", "  "); err == nil {
		out.Println(string(b))
	}
}

func (c *config) setDefaults() *config {
	if len(c.ChangeStreamNs) == 0 {
		c.ChangeStreamNs = []string{""}
	}
	if len(c.DirectReadNs) == 0 {
		c.DirectReadNs = []string{}
	}
	return c
}

func (c *config) validate() error {
	if c.MaxDuration != "" {
		if _, err := time.ParseDuration(c.MaxDuration); err != nil {
			return fmt.Errorf("Invalid MaxDuration: %s", err)
		}
	} else {
		return fmt.Errorf("MaxDuration cannot be empty")
	}
	if c.StatsDuration != "" {
		if _, err := time.ParseDuration(c.StatsDuration); err != nil {
			return fmt.Errorf("Invalid StatsDuration: %s", err)
		}
	} else {
		return fmt.Errorf("StatsDuration cannot be empty")
	}
	return nil
}

func (c *config) override(fc *config) {
	if !c.hasFlag("mongo") && fc.MongoURI != "" {
		c.MongoURI = fc.MongoURI
	}
	if !c.hasFlag("redisearch") && fc.RedisearchAddrs != "" {
		c.RedisearchAddrs = fc.RedisearchAddrs
	}
	if fc.DisableCreateIndex {
		c.DisableCreateIndex = true
	}
	if fc.FailFast {
		c.FailFast = true
	}
	if fc.ExitAfterDirectReads {
		c.ExitAfterDirectReads = true
	}
	if fc.DisableStats {
		c.DisableStats = true
	}
	if !c.hasFlag("max-items") && fc.MaxItems != 0 {
		c.MaxItems = fc.MaxItems
	}
	if !c.hasFlag("max-size") && fc.MaxSize != 0 {
		c.MaxSize = fc.MaxSize
	}
	if !c.hasFlag("max-duration") && fc.MaxDuration != "" {
		c.MaxDuration = fc.MaxDuration
	}
	if !c.hasFlag("stats-duration") && fc.StatsDuration != "" {
		c.StatsDuration = fc.StatsDuration
	}
	if !c.hasFlag("indexers") && fc.Indexers > 0 {
		c.Indexers = fc.Indexers
	}
	if len(c.ChangeStreamNs) == 0 {
		c.ChangeStreamNs = fc.ChangeStreamNs
	}
	if len(c.DirectReadNs) == 0 {
		c.DirectReadNs = fc.DirectReadNs
	}
	if !c.hasFlag("direct-read-split-max") && fc.DirectReadSplitMax != 0 {
		c.DirectReadSplitMax = fc.DirectReadSplitMax
	}
	if !c.hasFlag("direct-read-concur") && fc.DirectReadConcur != 0 {
		c.DirectReadConcur = fc.DirectReadConcur
	}
}

func mustConfig() *config {
	conf, err := loadConfig()
	if err != nil {
		errorLog.Fatalf("Configuration failed: %s", err)
		return nil
	}
	return conf
}

func parseFlags() *config {
	var c config
	var v bool
	flag.BoolVar(&v, "version", false, "Print the version number and exit")
	flag.BoolVar(&v, "v", false, "Print the version number and exit")
	flag.StringVar(&c.ConfigFile, "f", "", "Path to a TOML formatted config file")
	flag.StringVar(&c.MongoURI, "mongo", "mongodb://localhost:27017",
		"MongoDB connection string URI")
	flag.StringVar(&c.RedisearchAddrs, "redisearch", "localhost:6379",
		"Comma separated list of RediSearch host:port pairs")
	flag.BoolVar(&c.DisableCreateIndex, "disable-create-index", false,
		"True to disable auto-create attempts for missing RediSearch indexes")
	flag.BoolVar(&c.FailFast, "fail-fast", false,
		"True to exit the process after the first connection failure")
	flag.BoolVar(&c.ExitAfterDirectReads, "exit-after-direct-reads", false,
		"True to exit the process after direct reads have completed")
	flag.BoolVar(&c.DisableStats, "disable-stats", false,
		"True to disable periodic logging of indexing stats")
	flag.IntVar(&c.MaxItems, "max-items", 1000,
		"The max number of documents each indexer will buffer before forcing a flush")
	flag.Int64Var(&c.MaxSize, "max-size", 0,
		"The max number of bytes each indexer will accrue before forcing a flush")
	flag.StringVar(&c.MaxDuration, "max-duration", "1s",
		"The max duration each indexer will wait before forcing a flush")
	flag.StringVar(&c.StatsDuration, "stats-duration", "10s",
		"The max duration to wait before logging indexing stats")
	flag.IntVar(&c.Indexers, "indexers", 4,
		"The number of go routines concurrently indexing documents")
	flag.Var(&c.ChangeStreamNs, "change-stream-namespace", "MongoDB namespace to watch for changes")
	flag.Var(&c.DirectReadNs, "direct-read-namespace", "MongoDB namespace to read and sync")
	flag.IntVar(&c.DirectReadSplitMax, "direct-read-split-max", 9,
		"The max number of times to split each collection for concurrent reads")
	flag.IntVar(&c.DirectReadConcur, "direct-read-concur", 4,
		"The max number collections to read concurrently")
	flag.Parse()
	if v {
		fmt.Println(version)
		os.Exit(0)
	}
	return &c
}

func loadConfig() (*config, error) {
	c := parseFlags()
	if c.ConfigFile != "" {
		var fc config
		if md, err := toml.DecodeFile(c.ConfigFile, &fc); err != nil {
			return nil, err
		} else if ud := md.Undecoded(); len(ud) != 0 {
			return nil, fmt.Errorf("Config file contains undecoded keys: %q", ud)
		}
		c.override(&fc)
	}
	if err := c.setDefaults().validate(); err != nil {
		return nil, err
	}
	return c, nil
}

type indexStats struct {
	Total      int64
	Indexed    int64
	Deleted    int64
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
	config      *config
	namespace   string
	workQ       chan *gtm.Op
	indexers    int
	maxDuration time.Duration
	maxItems    int
	maxSize     int64
	stopC       chan bool
	allWg       *sync.WaitGroup
	logs        *loggers
	stats       *indexStats
	createIndex bool
	buffers     []*indexBuffer
}

type indexClient struct {
	config        *config
	readC         chan bool
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
	indexers      int
}

func (args *stringargs) String() string {
	return fmt.Sprintf("%s", *args)
}

func (args *stringargs) Set(value string) error {
	*args = append(*args, value)
	return nil
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
		Deleted: is.Deleted,
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

func (is *indexStats) addDeleted(count int) {
	is.Lock()
	defer is.Unlock()
	is.Deleted += int64(count)
}

func (is *indexStats) addFailed(count int) {
	is.Lock()
	defer is.Unlock()
	is.Failed += int64(count)
}

func (ib *indexBuffer) flatmap(prefix string, e map[string]interface{}) map[string]interface{} {
	o := make(map[string]interface{})
	for k, v := range e {
		switch child := v.(type) {
		case []interface{}:
			break
		case map[string]interface{}:
			nm := ib.flatmap("", child)
			for nk, nv := range nm {
				o[prefix+k+"."+nk] = nv
			}
		case time.Time:
			o[prefix+k] = child.Unix()
		default:
			o[prefix+k] = v
		}
	}
	return o
}

func (ib *indexBuffer) createDoc(op *gtm.Op) redisearch.Document {
	docId := opIDToString(op)
	doc := redisearch.NewDocument(docId, 1.0)
	for k, v := range op.Data {
		if k == "_id" {
			continue
		}
		switch val := v.(type) {
		case []interface{}:
			break
		case map[string]interface{}:
			flat := ib.flatmap(k+".", val)
			for fk, fv := range flat {
				switch fval := fv.(type) {
				case time.Time:
					doc.Set(fk, fval.Unix())
				default:
					doc.Set(fk, fv)
				}
			}
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
	if len(ib.items) == 0 {
		return sc
	}
	item := ib.items[0]
	if item.Properties == nil {
		return sc
	}
	for k, v := range item.Properties {
		switch val := v.(type) {
		case []interface{}:
			break
		case map[string]interface{}:
			flat := ib.flatmap(k+".", val)
			for fk, fv := range flat {
				switch fv.(type) {
				case time.Time, int, int32, int64, float32, float64:
					sc.AddField(redisearch.NewNumericField(fk))
				default:
					sc.AddField(redisearch.NewTextField(fk))
				}
			}
		case time.Time, int, int32, int64, float32, float64:
			sc.AddField(redisearch.NewNumericField(k))
		default:
			sc.AddField(redisearch.NewTextField(k))
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
		ib.logs.infoLog.Printf("Auto created index with schema: %s", string(b))
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
	if err != nil && ib.worker.config.DisableCreateIndex == false {
		// attempt to create schema and index on the fly
		// then retry indexing once
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

func (ib *indexBuffer) indexFailed(err error) {
	ib.logs.errorLog.Printf("Indexing failed: %s", err)
}

func (ib *indexBuffer) addItem(op *gtm.Op) {
	if op.Data == nil {
		return
	}
	doc := ib.createDoc(op)
	ib.items = append(ib.items, doc)
	if ib.maxSize != 0 {
		ib.curSize += int64(doc.EstimateSize())
	}
	if ib.full() {
		if err := ib.flush(); err != nil {
			ib.indexFailed(err)
		}
	}
}

func (ib *indexBuffer) run() {
	defer ib.worker.allWg.Done()
	timer := time.NewTicker(ib.maxDuration)
	defer timer.Stop()
	done := false
	for !done {
		select {
		case op := <-ib.worker.workQ:
			ib.addItem(op)
		case <-timer.C:
			if err := ib.flush(); err != nil {
				ib.indexFailed(err)
			}
		case <-ib.stopC:
			done = true
		}
	}
	if err := ib.flush(); err != nil {
		ib.indexFailed(err)
	}
}

func (iw *indexWorker) start() {
	for i := 0; i < iw.indexers; i++ {
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
		iw.buffers = append(iw.buffers, buf)
		go buf.run()
	}
}

func (iw *indexWorker) add(op *gtm.Op) *indexWorker {
	iw.workQ <- op
	return iw
}

func (iw *indexWorker) remove(op *gtm.Op) *indexWorker {
	// since there are no version numbers to maintain sequence do the best we can.
	// flush all pending index requests and immediately do the delete
	for _, ib := range iw.buffers {
		if err := ib.flush(); err != nil {
			ib.indexFailed(err)
		}
	}
	client := iw.client
	docId := opIDToString(op)
	iw.stats.addTotal(1)
	if err := client.Delete(docId, true); err == nil {
		iw.stats.addDeleted(1)
	} else {
		iw.stats.addFailed(1)
		iw.logs.errorLog.Printf("Delete failed: %s", err)
	}
	return iw
}

func newLoggers() *loggers {
	return &loggers{
		infoLog:  infoLog,
		warnLog:  warnLog,
		statsLog: statsLog,
		traceLog: traceLog,
		errorLog: errorLog,
	}
}

func newIndexClient(ctx *gtm.OpCtx, conf *config) *indexClient {
	maxDuration, _ := time.ParseDuration(conf.MaxDuration)
	statsDuration, _ := time.ParseDuration(conf.StatsDuration)
	return &indexClient{
		config:        conf,
		indexers:      conf.Indexers,
		readC:         make(chan bool),
		readContext:   ctx,
		allWg:         &sync.WaitGroup{},
		stopC:         make(chan bool),
		addrs:         conf.RedisearchAddrs,
		redisClients:  make(map[string]*redisearch.Client),
		workers:       make(map[string]*indexWorker),
		maxDuration:   maxDuration,
		maxItems:      conf.MaxItems,
		maxSize:       conf.MaxSize,
		logs:          newLoggers(),
		stats:         &indexStats{},
		createIndex:   true,
		statsDuration: statsDuration,
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
	ic.stop()
	os.Exit(0)
}

func (ic *indexClient) logStats() {
	stats := ic.stats.dup()
	if b, err := json.Marshal(stats); err == nil {
		ic.logs.statsLog.Println(string(b))
	}
}

func (ic *indexClient) statsLoop() {
	if ic.config.DisableStats {
		return
	}
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

func (ic *indexClient) readListen() {
	conf := ic.config
	if len(conf.DirectReadNs) > 0 {
		ic.readContext.DirectReadWg.Wait()
		infoLog.Println("Direct reads completed")
		if conf.ExitAfterDirectReads {
			ic.logs.infoLog.Println("Shutting down")
			ic.stop()
			os.Exit(0)
		}
	}
}

func (client *indexClient) eventLoop() {
	go client.sigListen()
	go client.readListen()
	go client.statsLoop()
	ctx := client.readContext
	drained := false
	for {
		select {
		case err := <-ctx.ErrC:
			if err == nil {
				break
			}
			client.logs.errorLog.Println(err)
		case op, open := <-ctx.OpC:
			if op == nil {
				if !open && !drained {
					drained = true
					close(client.readC)
				}
				break
			}
			client.queue(op)
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

func (ic *indexClient) setIndexers(count int) *indexClient {
	ic.indexers = count
	return ic
}

func (ic *indexClient) setMaxItems(max int) *indexClient {
	ic.maxItems = max
	return ic
}

func (ic *indexClient) setMaxSize(max int64) *indexClient {
	ic.maxSize = max
	return ic
}

func (ic *indexClient) setMaxDuration(max time.Duration) *indexClient {
	ic.maxDuration = max
	return ic
}

func (ic *indexClient) setStatsDuration(max time.Duration) *indexClient {
	ic.statsDuration = max
	return ic
}

func (ic *indexClient) stop() {
	ic.readContext.Stop()
	<-ic.readC
	close(ic.stopC)
	ic.allWg.Wait()
}

func (ic *indexClient) queue(op *gtm.Op) *indexClient {
	ns := op.Namespace
	c := ic.redisClients[ns]
	if c == nil {
		c = redisearch.NewClient(ic.addrs, ns)
		ic.redisClients[ns] = c
	}
	worker := ic.workers[ns]
	if worker == nil {
		worker = &indexWorker{
			config:      ic.config,
			client:      c,
			indexers:    ic.indexers,
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
	if op.IsInsert() || op.IsUpdate() {
		worker.add(op)
	} else if op.IsDelete() {
		worker.remove(op)
	}
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
		return nil, fmt.Errorf("MongoDB connection failed: %s", err)
	}
	ctx1, cancel1 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel1()
	if err = client.Connect(ctx1); err != nil {
		return nil, fmt.Errorf("MongoDB connection failed: %s", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel2()
	if err = client.Ping(ctx2, nil); err != nil {
		return nil, fmt.Errorf("MongoDB connection failed: %s", err)
	}
	return client, nil
}

func mustConnect(conf *config) *mongo.Client {
	var (
		client    *mongo.Client
		connected bool
		err       error
	)
	for !connected {
		client, err = dialMongo(conf.MongoURI)
		if err == nil {
			connected = true
		} else {
			if conf.FailFast {
				errorLog.Fatalf("MongoDB connection failed: %s", err)
				break
			} else {
				errorLog.Println(err)
			}
		}
	}
	return client
}

func main() {
	var (
		client *mongo.Client
		conf   *config
	)
	conf = mustConfig()
	conf.log(infoLog)
	client = mustConnect(conf)
	defer client.Disconnect(context.Background())
	ctx := gtm.Start(client, &gtm.Options{
		ChangeStreamNs:     conf.ChangeStreamNs,
		DirectReadNs:       conf.DirectReadNs,
		DirectReadConcur:   conf.DirectReadConcur,
		DirectReadSplitMax: int32(conf.DirectReadSplitMax),
		OpLogDisabled:      true,
	})
	indexClient := newIndexClient(ctx, conf)
	indexClient.eventLoop()
}
