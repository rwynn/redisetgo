package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/rwynn/gtm"
	"github.com/rwynn/redisetgo/module"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"plugin"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	version = "1.0.1"
)

var (
	infoLog  = log.New(os.Stderr, "INFO ", log.Flags())
	warnLog  = log.New(os.Stdout, "WARN ", log.Flags())
	statsLog = log.New(os.Stdout, "STATS ", log.Flags())
	traceLog = log.New(os.Stdout, "TRACE ", log.Flags())
	errorLog = log.New(os.Stderr, "ERROR ", log.Flags())
)

type stringargs []string

type defaultHandler struct {
	name string
}

func (df *defaultHandler) Name() string {
	return df.name
}

func (df *defaultHandler) Handles(ev *module.Event) bool {
	return true
}

func (df *defaultHandler) OnInsert(ev *module.InsertEvent) (*module.IndexResponse, error) {
	doc := createDoc(ev.Op)
	return &module.IndexResponse{
		ToIndex:  []redisearch.Document{doc},
		Finished: true,
	}, nil
}

func (df *defaultHandler) OnUpdate(ev *module.UpdateEvent) (*module.IndexResponse, error) {
	doc := createDoc(ev.Op)
	return &module.IndexResponse{
		ToIndex:  []redisearch.Document{doc},
		Finished: true,
	}, nil
}

func (df *defaultHandler) OnDelete(ev *module.DeleteEvent) (*module.DeleteResponse, error) {
	docId := toDocID(ev.Op)
	return &module.DeleteResponse{
		ToDelete: []string{docId},
		Finished: true,
	}, nil
}

type eventHandlerRef struct {
	path string
	impl module.EventHandler
}

type config struct {
	ConfigFile           string     `json:"config-file"`
	MongoURI             string     `toml:"mongo" json:"mongo"`
	RedisearchAddrs      string     `toml:"redisearch" json:"redisearch"`
	DisableCreateIndex   bool       `toml:"disable-create-index" json:"disable-create-index"`
	MaxItems             int        `toml:"max-items" json:"max-items"`
	MaxSize              int64      `toml:"max-size" json:"max-size"`
	MaxDuration          string     `toml:"max-duration" json:"max-duration"`
	MaxRetries           int        `toml:"max-retries" json:"max-retries"`
	RetryDuration        string     `toml:"retry-duration" json:"retry-duration"`
	StatsDuration        string     `toml:"stats-duration" json:"stats-duration"`
	Indexers             int        `toml:"indexers" json:"indexers"`
	ChangeStreamNs       stringargs `toml:"change-stream-namespaces" json:"change-stream-namespaces"`
	DirectReadNs         stringargs `toml:"direct-read-namespaces" json:"direct-read-namespaces"`
	DirectReadSplitMax   int        `toml:"direct-read-split-max" json:"direct-read-split-max"`
	DirectReadConcur     int        `toml:"direct-read-concur" json:"direct-read-concur"`
	FailFast             bool       `toml:"fail-fast" json:"fail-fast"`
	ExitAfterDirectReads bool       `toml:"exit-after-direct-reads" json:"exit-after-direct-reads"`
	DisableStats         bool       `toml:"disable-stats" json:"disable-stats"`
	Resume               bool       `toml:"resume" json:"resume"`
	ResumeName           string     `toml:"resume-name" json:"resume-name"`
	MetadataDB           string     `toml:"metadata-db" json:"metadata-db"`
	PluginFiles          stringargs `toml:"plugins" json:"plugins"`
	EnableHTTPServer     bool       `toml:"http-server" json:"http-server"`
	HTTPServerAddr       string     `toml:"http-server-addr" json:"http-server-addr"`
	Language             string     `toml:"language" json:"language"`
	Pprof                bool       `toml:"pprof" json:"pprof"`
	eventHandlers        []eventHandlerRef
	schemaHandlers       []module.SchemaHandler
	pipeBuilders         []module.PipeBuilder
	viewConfig           bool
}

func (c *config) buildPipe() gtm.PipelineBuilder {
	if len(c.pipeBuilders) == 0 {
		return nil
	}
	return func(namespace string, changeStream bool) ([]interface{}, error) {
		pr := &module.PipeRequest{
			Namespace:    namespace,
			ChangeStream: changeStream,
		}
		for _, pb := range c.pipeBuilders {
			resp, err := pb.BuildPipeline(pr)
			if err != nil {
				errorLog.Printf("Error in pipeline builder[%s]: %s", pb.Name(), err)
				continue
			}
			if resp != nil && len(resp.Stages) > 0 {
				var stages []interface{}
				for _, stage := range resp.Stages {
					stages = append(stages, stage)
				}
				return stages, nil
			}
		}
		return nil, nil
	}
}

func (c *config) nsFilter() gtm.OpFilter {
	db := c.MetadataDB
	return func(op *gtm.Op) bool {
		return op.GetDatabase() != db
	}
}

func (c *config) resumeFunc() gtm.TimestampGenerator {
	if !c.Resume {
		return nil
	}
	return func(client *mongo.Client, opts *gtm.Options) (primitive.Timestamp, error) {
		var ts primitive.Timestamp
		col := client.Database(c.MetadataDB).Collection("resume")
		result := col.FindOne(context.Background(), bson.M{
			"_id": c.ResumeName,
		})
		if err := result.Err(); err == nil {
			doc := make(map[string]interface{})
			if err = result.Decode(&doc); err == nil {
				if doc["ts"] != nil {
					ts = doc["ts"].(primitive.Timestamp)
					ts.I += 1
				}
			}
		}
		if ts.T == 0 {
			ts, _ = gtm.LastOpTimestamp(client, opts)
		}
		infoLog.Printf("Resuming from timestamp %+v", ts)
		return ts, nil
	}
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

func (c *config) log(out io.Writer) {
	if b, err := json.MarshalIndent(c, "", "  "); err == nil {
		out.Write(b)
		out.Write([]byte("\n"))
	}
}

func (c *config) setDefaults() *config {
	if len(c.ChangeStreamNs) == 0 {
		c.ChangeStreamNs = []string{""}
	}
	if len(c.DirectReadNs) == 0 {
		c.DirectReadNs = []string{}
	}
	if len(c.schemaHandlers) == 0 {
		c.schemaHandlers = []module.SchemaHandler{}
	}
	if len(c.pipeBuilders) == 0 {
		c.pipeBuilders = []module.PipeBuilder{}
	}
	if len(c.PluginFiles) == 0 {
		c.PluginFiles = []string{}
	}
	ehr := eventHandlerRef{
		path: "main",
		impl: &defaultHandler{name: "redisetgo"},
	}
	c.eventHandlers = append(c.eventHandlers, ehr)
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
	if c.RetryDuration != "" {
		if _, err := time.ParseDuration(c.RetryDuration); err != nil {
			return fmt.Errorf("Invalid RetryDuration: %s", err)
		}
	} else {
		return fmt.Errorf("RetryDuration cannot be empty")
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
	if fc.Pprof {
		c.Pprof = true
	}
	if fc.EnableHTTPServer {
		c.EnableHTTPServer = true
	}
	if !c.hasFlag("language") && fc.Language != "" {
		c.Language = fc.Language
	}
	if !c.hasFlag("http-server-addr") && fc.HTTPServerAddr != "" {
		c.HTTPServerAddr = fc.HTTPServerAddr
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
	if fc.Resume {
		c.Resume = true
	}
	if !c.hasFlag("metadata-db") && fc.MetadataDB != "" {
		c.MetadataDB = fc.MetadataDB
	}
	if !c.hasFlag("resume-name") && fc.ResumeName != "" {
		c.ResumeName = fc.ResumeName
	}
	if !c.hasFlag("max-items") && fc.MaxItems != 0 {
		c.MaxItems = fc.MaxItems
	}
	if !c.hasFlag("max-size") && fc.MaxSize != 0 {
		c.MaxSize = fc.MaxSize
	}
	if !c.hasFlag("max-retries") && fc.MaxRetries != -1 {
		c.MaxRetries = fc.MaxRetries
	}
	if !c.hasFlag("max-duration") && fc.MaxDuration != "" {
		c.MaxDuration = fc.MaxDuration
	}
	if !c.hasFlag("stats-duration") && fc.StatsDuration != "" {
		c.StatsDuration = fc.StatsDuration
	}
	if !c.hasFlag("retry-duration") && fc.RetryDuration != "" {
		c.RetryDuration = fc.RetryDuration
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
	if len(c.PluginFiles) == 0 {
		c.PluginFiles = fc.PluginFiles
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
	flag.BoolVar(&c.viewConfig, "view-config", false, "Print the configuration and exit")
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
	flag.BoolVar(&c.Resume, "resume", false,
		"True to resume indexing from last saved timestamp")
	flag.StringVar(&c.ResumeName, "resume-name", "default",
		"Key to store and load saved timestamps from")
	flag.StringVar(&c.MetadataDB, "metadata-db", "redisetgo",
		"Name of the MongoDB database to store redisetgo metadata")
	flag.IntVar(&c.MaxRetries, "max-retries", 10,
		"The max number of times an indexing request will be retried on a network failure")
	flag.IntVar(&c.MaxItems, "max-items", 1000,
		"The max number of documents each indexer will buffer before forcing a flush")
	flag.Int64Var(&c.MaxSize, "max-size", 0,
		"The max number of bytes each indexer will accrue before forcing a flush")
	flag.StringVar(&c.MaxDuration, "max-duration", "1s",
		"The max duration each indexer will wait before forcing a flush")
	flag.StringVar(&c.StatsDuration, "stats-duration", "10s",
		"The max duration to wait before logging indexing stats")
	flag.StringVar(&c.RetryDuration, "retry-duration", "5s",
		"The max duration to wait before retrying network errors")
	flag.IntVar(&c.Indexers, "indexers", 4,
		"The number of go routines concurrently indexing documents")
	flag.Var(&c.ChangeStreamNs, "change-stream-namespace", "MongoDB namespace to watch for changes")
	flag.Var(&c.DirectReadNs, "direct-read-namespace", "MongoDB namespace to read and sync")
	flag.Var(&c.PluginFiles, "plugin", "Path to .so redisetgo plugin file to load")
	flag.IntVar(&c.DirectReadSplitMax, "direct-read-split-max", 9,
		"The max number of times to split each collection for concurrent reads")
	flag.IntVar(&c.DirectReadConcur, "direct-read-concur", 4,
		"The max number collections to read concurrently")
	flag.BoolVar(&c.EnableHTTPServer, "http-server", false,
		"True to enable a HTTP server")
	flag.StringVar(&c.HTTPServerAddr, "http-server-addr", ":8080",
		"The address the HTTP server will bind to")
	flag.StringVar(&c.Language, "language", "",
		"The language to index for.  Defaults to english. See RediSearch docs for available values")
	flag.BoolVar(&c.Pprof, "pprof", false,
		"True to enable profiling support")
	flag.Parse()
	if v {
		fmt.Println(version)
		os.Exit(0)
	}
	return &c
}

func (c *config) loadPluginFile(pf string) error {
	p, err := plugin.Open(pf)
	if err != nil {
		return fmt.Errorf("Unable to load plugin %s: %s", pf, err)
	}
	ps, err := p.Lookup("InitPlugin")
	if err != nil {
		return fmt.Errorf("Symbol `InitPlugin` not found in plugin %s", pf)
	}
	var pfunc module.PluginInitializer
	var ok bool
	pfunc, ok = ps.(module.PluginInitializer)
	if !ok {
		return fmt.Errorf("Symbol `InitPlugin` must be type %T but was type %T", pfunc, ps)
	}
	plug := pfunc.Get()
	ehs := plug.Events
	if ehs != nil {
		for _, eh := range ehs {
			ehr := eventHandlerRef{
				path: pf,
				impl: eh,
			}
			c.eventHandlers = append(c.eventHandlers, ehr)
		}
	}
	sh := plug.Schemas
	if sh != nil {
		c.schemaHandlers = append(c.schemaHandlers, sh)
	}
	pb := plug.Pipeline
	if pb != nil {
		c.pipeBuilders = append(c.pipeBuilders, pb)
	}
	return nil
}

func loadConfig() (*config, error) {
	c := parseFlags()
	if c.ConfigFile != "" {
		fc := config{MaxRetries: -1}
		if md, err := toml.DecodeFile(c.ConfigFile, &fc); err != nil {
			return nil, err
		} else if ud := md.Undecoded(); len(ud) != 0 {
			return nil, fmt.Errorf("Config file contains undecoded keys: %q", ud)
		}
		c.override(&fc)
	}
	if len(c.PluginFiles) > 0 {
		for _, pf := range c.PluginFiles {
			if err := c.loadPluginFile(pf); err != nil {
				return nil, err
			}
		}
	}
	if err := c.setDefaults().validate(); err != nil {
		return nil, err
	}
	return c, nil
}

type indexStats struct {
	Indexed    int64 `json:"indexed"`
	Deleted    int64 `json:"deleted"`
	Failed     int64 `json:"failed"`
	Flushed    int64 `json:"flushed"`
	Retried    int64 `json:"retried"`
	Queued     int64 `json:"queued"`
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
	curSize       int64
	netErrors     int
	retryWg       *sync.WaitGroup
}

type indexWorker struct {
	client        *redisearch.Client
	mongoClient   *mongo.Client
	config        *config
	namespace     string
	workQ         chan *gtm.Op
	indexers      int
	maxDuration   time.Duration
	retryDuration time.Duration
	maxItems      int
	maxSize       int64
	stopC         chan bool
	allWg         *sync.WaitGroup
	logs          *loggers
	stats         *indexStats
	createIndex   bool
	buffers       []*indexBuffer
	schema        *redisearch.Schema
}

type indexClient struct {
	mongoClient   *mongo.Client
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
	retryDuration time.Duration
	indexers      int
	timestamp     primitive.Timestamp
	started       time.Time
	httpServer    *http.Server
	sync.Mutex
}

func (args *stringargs) String() string {
	return fmt.Sprintf("%s", *args)
}

func (args *stringargs) Set(value string) error {
	*args = append(*args, value)
	return nil
}

func toDocID(op *gtm.Op) string {
	var id strings.Builder
	id.WriteString(op.Namespace)
	id.WriteString(".")
	switch val := op.Id.(type) {
	case primitive.ObjectID:
		id.WriteString(val.Hex())
	case float64:
		intID := int(val)
		if op.Id.(float64) == float64(intID) {
			fmt.Fprintf(&id, "%v", intID)
		} else {
			fmt.Fprintf(&id, "%v", op.Id)
		}
	case float32:
		intID := int(val)
		if op.Id.(float32) == float32(intID) {
			fmt.Fprintf(&id, "%v", intID)
		} else {
			fmt.Fprintf(&id, "%v", op.Id)
		}
	default:
		fmt.Fprintf(&id, "%v", op.Id)
	}
	return id.String()
}

func (is *indexStats) dup() *indexStats {
	is.Lock()
	defer is.Unlock()
	return &indexStats{
		Flushed: is.Flushed,
		Indexed: is.Indexed,
		Deleted: is.Deleted,
		Failed:  is.Failed,
		Retried: is.Retried,
		Queued:  is.Queued,
	}
}

func (is *indexStats) addFlushed() {
	is.Lock()
	is.Flushed++
	is.Unlock()
}

func (is *indexStats) addIndexed(count int) {
	is.Lock()
	is.Indexed += int64(count)
	is.Unlock()
}

func (is *indexStats) addDeleted(count int) {
	is.Lock()
	is.Deleted += int64(count)
	is.Unlock()
}

func (is *indexStats) addFailed(count int) {
	is.Lock()
	is.Failed += int64(count)
	is.Unlock()
}

func (is *indexStats) addRetried(count int) {
	is.Lock()
	is.Retried += int64(count)
	is.Unlock()
}

func (is *indexStats) addQueued(count int) {
	is.Lock()
	is.Queued += int64(count)
	is.Unlock()
}

func flatmap(prefix string, e map[string]interface{}) map[string]interface{} {
	o := make(map[string]interface{})
	for k, v := range e {
		switch child := v.(type) {
		case []interface{}:
			break
		case map[string]interface{}:
			nm := flatmap("", child)
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

func createDoc(op *gtm.Op) redisearch.Document {
	docId := toDocID(op)
	doc := redisearch.NewDocument(docId, 1.0)
	for k, v := range op.Data {
		if k == "_id" {
			continue
		}
		switch val := v.(type) {
		case []interface{}:
			break
		case map[string]interface{}:
			flat := flatmap(k+".", val)
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
	sc := ib.worker.schema
	if sc != nil {
		return sc
	}
	sc = redisearch.NewSchema(redisearch.DefaultOptions)
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
			flat := flatmap(k+".", val)
			for fk, fv := range flat {
				switch fv.(type) {
				case time.Time, int, int32, int64, float32, float64:
					sc.AddField(redisearch.NewSortableNumericField(fk))
				default:
					sc.AddField(redisearch.NewSortableTextField(fk, 1.0))
				}
			}
		case time.Time, int, int32, int64, float32, float64:
			sc.AddField(redisearch.NewSortableNumericField(k))
		default:
			sc.AddField(redisearch.NewSortableTextField(k, 1.0))
		}
	}
	return sc
}

func (ib *indexBuffer) afterFlush(err error) {
	itemCount := len(ib.items)
	ib.stats.addFlushed()
	if err == nil {
		ib.items = nil
		ib.stats.addIndexed(itemCount)
		ib.stats.addQueued(-1 * itemCount)
	} else if !isNetError(err) {
		multiError, ok := err.(redisearch.MultiError)
		if ok {
			var indexed int
			var failedItems []redisearch.Document
			for i, e := range multiError {
				if e == nil {
					indexed++
				} else {
					failedItems = append(failedItems, ib.items[i])
				}
			}
			ib.items = failedItems
			ib.stats.addIndexed(indexed)
			ib.stats.addQueued(-1 * indexed)
		}
	}
	ib.curSize = 0
	if ib.items != nil && ib.maxSize != 0 {
		for _, doc := range ib.items {
			ib.curSize += int64(doc.EstimateSize())
		}
	}
}

func logIndexInfo(indexInfo *redisearch.IndexInfo) {
	if b, err := json.Marshal(indexInfo); err == nil {
		infoLog.Printf("Auto created index with schema: %s", string(b))
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

func isNetError(err error) bool {
	if err == nil {
		return false
	}
	multiError, ok := err.(redisearch.MultiError)
	if ok {
		for _, e := range multiError {
			if isNetError(e) {
				return true
			}
		}
	} else {
		switch t := err.(type) {
		case net.Error:
			return true
		case *net.OpError:
			return true
		case syscall.Errno:
			if t == syscall.ECONNREFUSED {
				return true
			}
		}
	}
	return false
}

func (ib *indexBuffer) retryOk(err error) bool {
	return isNetError(err) && ib.netErrors < ib.worker.config.MaxRetries
}

func (ib *indexBuffer) inRetry() bool {
	return ib.netErrors > 0
}

func (ib *indexBuffer) flush() (err error) {
	if len(ib.items) == 0 {
		return
	}
	docs := ib.items
	client := ib.worker.client
	config := ib.worker.config
	indexOptions := redisearch.IndexingOptions{Replace: true, Language: config.Language}
	err = client.IndexOptions(indexOptions, docs...)
	if err != nil && ib.worker.createIndex {
		// attempt to create schema and index on the fly
		// then retry indexing once
		if _, ie := client.Info(); ie != nil {
			if indexInfo, cie := ib.autoCreateIndex(); cie == nil {
				logIndexInfo(indexInfo)
			}
			err = client.IndexOptions(indexOptions, docs...)
		}
	}
	ib.afterFlush(err)
	ib.checkRetry(err)
	return
}

func (ib *indexBuffer) checkRetry(err error) {
	maxRetries := ib.worker.config.MaxRetries
	if maxRetries > 0 {
		if ib.retryOk(err) {
			ib.netErrors += 1
			ib.stats.addRetried(len(ib.items))
		} else {
			if ib.inRetry() {
				errorLog.Println("Max retries reached. Dropping queued events to keep pipeline healthy")
			}
			ib.discard()
		}
	} else {
		ib.discard()
	}
}

func (ib *indexBuffer) discard() {
	itemCount := len(ib.items)
	ib.netErrors = 0
	ib.stats.addFailed(itemCount)
	ib.stats.addQueued(-1 * itemCount)
	ib.items = nil
	ib.curSize = 0
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
	logged := err
	if multiError, ok := err.(redisearch.MultiError); ok {
		for _, e := range multiError {
			if e != nil && e.Error() == "Unknown index name" {
				logged = e
				break
			}
		}
	}
	ib.logs.errorLog.Printf("Indexing failed: %s", logged)
}

func (ib *indexBuffer) addItem(doc redisearch.Document) {
	ib.items = append(ib.items, doc)
	ib.worker.stats.addQueued(1)
	if ib.maxSize != 0 {
		ib.curSize += int64(doc.EstimateSize())
	}
	if ib.full() {
		if err := ib.flush(); err != nil {
			ib.indexFailed(err)
		}
	}
}

func (ib *indexBuffer) handleInsert(op *gtm.Op) {
	if op.Data == nil {
		ib.stats.addFailed(1)
		errorLog.Println("Insert event had no associated data")
		return
	}
	config := ib.worker.config
	for _, ref := range config.eventHandlers {
		plugin := ref.impl
		ev := &module.Event{Op: op}
		if !plugin.Handles(ev) {
			continue
		}
		req := &module.InsertEvent{}
		req.MongoClient = ib.worker.mongoClient
		req.RedisSearchClient = ib.worker.client
		req.Op = op
		req.Logs = &module.Loggers{
			InfoLog:  infoLog,
			WarnLog:  warnLog,
			TraceLog: traceLog,
			ErrorLog: errorLog,
		}
		resp, err := plugin.OnInsert(req)
		if err != nil {
			ib.stats.addFailed(1)
			errorLog.Printf("Insert handler for plugin %s[%s] returned error: %s", plugin.Name(), ref.path, err)
			break
		}
		if resp != nil {
			docs := resp.ToIndex
			if docs != nil {
				for _, doc := range docs {
					ib.addItem(doc)
				}
			}
			if resp.Finished {
				break
			}
		}
	}
}

func (ib *indexBuffer) handleUpdate(op *gtm.Op) {
	if op.Data == nil {
		ib.stats.addFailed(1)
		errorLog.Println("Update event had no associated data")
		return
	}
	config := ib.worker.config
	for _, ref := range config.eventHandlers {
		plugin := ref.impl
		ev := &module.Event{Op: op}
		if !plugin.Handles(ev) {
			continue
		}
		req := &module.UpdateEvent{}
		req.MongoClient = ib.worker.mongoClient
		req.RedisSearchClient = ib.worker.client
		req.Op = op
		req.Logs = &module.Loggers{
			InfoLog:  infoLog,
			WarnLog:  warnLog,
			TraceLog: traceLog,
			ErrorLog: errorLog,
		}
		resp, err := plugin.OnUpdate(req)
		if err != nil {
			ib.stats.addFailed(1)
			errorLog.Printf("Update handler for plugin %s[%s] returned error: %s", plugin.Name(), ref.path, err)
			break
		}
		if resp != nil {
			docs := resp.ToIndex
			if docs != nil {
				for _, doc := range docs {
					ib.addItem(doc)
				}
			}
			if resp.Finished {
				break
			}
		}
	}
}

func (iw *indexWorker) handleDelete(op *gtm.Op) redisearch.MultiError {
	var errors redisearch.MultiError
	config := iw.config
	client := iw.client
	for _, ref := range config.eventHandlers {
		plugin := ref.impl
		ev := &module.Event{Op: op}
		if !plugin.Handles(ev) {
			continue
		}
		req := &module.DeleteEvent{}
		req.MongoClient = iw.mongoClient
		req.RedisSearchClient = client
		req.Op = op
		req.Logs = &module.Loggers{
			InfoLog:  infoLog,
			WarnLog:  warnLog,
			TraceLog: traceLog,
			ErrorLog: errorLog,
		}
		resp, err := plugin.OnDelete(req)
		if err != nil {
			errors = append(errors, err)
			iw.stats.addFailed(1)
			errorLog.Printf("Delete handler for plugin %s[%s] returned error: %s", plugin.Name(), ref.path, err)
			break
		}
		if resp != nil {
			docIds := resp.ToDelete
			if docIds != nil {
				iw.stats.addQueued(len(docIds))
				for _, docId := range docIds {
					if err := client.Delete(docId, true); err == nil {
						iw.stats.addDeleted(1)
					} else {
						errors = append(errors, err)
						iw.stats.addFailed(1)
						iw.logs.errorLog.Printf("Delete of document ID %s failed: %s", docId, err)
					}
					iw.stats.addQueued(-1)
				}
			}
			if resp.Finished {
				break
			}
		}
	}
	if len(errors) > 0 {
		return errors
	}
	return nil
}

func (ib *indexBuffer) handleEvent(op *gtm.Op) {
	if op.IsInsert() {
		ib.handleInsert(op)
	} else {
		ib.handleUpdate(op)
	}
}

func (ib *indexBuffer) retryLoop() {
	defer ib.retryWg.Done()
	retryFlush := time.NewTicker(ib.worker.retryDuration)
	defer retryFlush.Stop()
	done := false
	for !done {
		select {
		case <-retryFlush.C:
			if err := ib.flush(); err != nil {
				ib.indexFailed(err)
			}
			done = ib.netErrors == 0
		case <-ib.stopC:
			done = true
		}
	}
}

func (ib *indexBuffer) retryWait() {
	retry := ib.worker.config.MaxRetries > 0
	if retry && ib.netErrors > 0 {
		ib.retryWg.Add(1)
		go ib.retryLoop()
		ib.retryWg.Wait()
	}
}

func (ib *indexBuffer) run() {
	defer ib.worker.allWg.Done()
	autoFlush := time.NewTicker(ib.maxDuration)
	defer autoFlush.Stop()
	done := false
	for !done {
		select {
		case op := <-ib.worker.workQ:
			ib.handleEvent(op)
			ib.retryWait()
		case <-autoFlush.C:
			if err := ib.flush(); err != nil {
				ib.indexFailed(err)
			}
			ib.retryWait()
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
			retryWg:     &sync.WaitGroup{},
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
	buffersEmpty := 0
	for {
		for _, ib := range iw.buffers {
			if err := ib.flush(); err != nil {
				ib.indexFailed(err)
			} else {
				buffersEmpty += 1
			}
		}
		if buffersEmpty == len(iw.buffers) {
			break
		}
		buffersEmpty = 0
	}
	netErrors := 0
	for {
		if err := iw.handleDelete(op); err == nil || !isNetError(err) {
			break
		} else {
			iw.stats.addRetried(len(err))
			netErrors += 1
			if netErrors >= iw.config.MaxRetries {
				break
			}
		}
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

func newIndexClient(client *mongo.Client, ctx *gtm.OpCtx, conf *config) *indexClient {
	maxDuration, _ := time.ParseDuration(conf.MaxDuration)
	statsDuration, _ := time.ParseDuration(conf.StatsDuration)
	retryDuration, _ := time.ParseDuration(conf.RetryDuration)
	createIndex := conf.DisableCreateIndex == false
	return &indexClient{
		mongoClient:   client,
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
		createIndex:   createIndex,
		statsDuration: statsDuration,
		retryDuration: retryDuration,
	}
}

func (ic *indexClient) sigListen() {
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer signal.Stop(sigs)
	<-sigs
	go func() {
		<-sigs
		ic.logs.infoLog.Println("Forcing shutdown, bye bye...")
		os.Exit(1)
	}()
	ic.logs.infoLog.Println("Shutting down")
	ic.stop()
	ic.logs.infoLog.Println("Ready to exit, bye bye...")
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

func (client *indexClient) getTimestamp() primitive.Timestamp {
	client.Lock()
	defer client.Unlock()
	return client.timestamp
}

func (client *indexClient) setTimestamp(ts primitive.Timestamp) {
	client.Lock()
	client.timestamp = ts
	client.Unlock()
}

func (client *indexClient) saveTimestamp(ts primitive.Timestamp) error {
	config := client.config
	mclient := client.mongoClient
	if ts.T == 0 {
		return nil
	}
	col := mclient.Database(config.MetadataDB).Collection("resume")
	doc := bson.M{
		"ts": ts,
	}
	opts := options.Update()
	opts.SetUpsert(true)
	_, err := col.UpdateOne(context.Background(), bson.M{
		"_id": config.ResumeName,
	}, bson.M{
		"$set": doc,
	}, opts)
	return err
}

func (client *indexClient) timestampLoop() {
	ticker := time.NewTicker(time.Duration(10) * time.Second)
	defer ticker.Stop()
	running := true
	lastTime := primitive.Timestamp{}
	for running {
		select {
		case <-ticker.C:
			ts := client.getTimestamp()
			if ts.T > lastTime.T || (ts.T == lastTime.T && ts.I > lastTime.I) {
				lastTime = ts
				if err := client.saveTimestamp(lastTime); err != nil {
					errorLog.Printf("Error saving timestamp: %s", err)
				}
			}
			break
		case <-client.stopC:
			running = false
			break
		}
	}
}

func (client *indexClient) metaLoop() {
	config := client.config
	if config.Resume {
		go client.timestampLoop()
	}
}

func (client *indexClient) eventLoop() {
	go client.serveHttp()
	go client.sigListen()
	go client.readListen()
	go client.statsLoop()
	client.metaLoop()
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

func (ic *indexClient) serveHttp() {
	config := ic.config
	if !config.EnableHTTPServer {
		return
	}
	ic.buildServer()
	s := ic.httpServer
	infoLog.Printf("Starting http server at %s", s.Addr)
	ic.started = time.Now()
	err := s.ListenAndServe()
	select {
	case <-ic.stopC:
		return
	default:
		errorLog.Fatalf("Unable to serve http at address %s: %s", s.Addr, err)
	}
}

func (ic *indexClient) buildServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/started", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		data := (time.Now().Sub(ic.started)).String()
		w.Write([]byte(data))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	})
	if !ic.config.DisableStats {
		mux.HandleFunc("/stats", func(w http.ResponseWriter, req *http.Request) {
			stats, err := json.MarshalIndent(ic.stats.dup(), "", "    ")
			if err == nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
				w.Write(stats)
			} else {
				w.WriteHeader(500)
				fmt.Fprintf(w, "Unable to print statistics: %s", err)
			}
		})
	}
	if ic.config.Pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	s := &http.Server{
		Addr:     ic.config.HTTPServerAddr,
		Handler:  mux,
		ErrorLog: errorLog,
	}
	ic.httpServer = s
}

func (ic *indexClient) queue(op *gtm.Op) *indexClient {
	var schema *redisearch.Schema
	ns := op.Namespace
	c := ic.redisClients[ns]
	if c == nil {
		indexName := ns
		schemaEvent := &module.SchemaEvent{
			Namespace: ns,
		}
		for _, sh := range ic.config.schemaHandlers {
			if sh.Handles(schemaEvent) {
				sr, err := sh.Schema(schemaEvent)
				if err != nil {
					errorLog.Printf("Error calling schema handler %s:%s", sh.Name(), err)
					return ic
				}
				if sr != nil {
					if sr.Index != "" {
						indexName = sr.Index
					}
					if sr.Schema != nil {
						schema = sr.Schema
					}
				}
				break
			}
		}
		c = redisearch.NewClient(ic.addrs, indexName)
		if schema != nil {
			// check and create plugin provided schema
			_, err := c.Info()
			if err != nil {
				var indexInfo *redisearch.IndexInfo
				if err = c.CreateIndex(schema); err == nil {
					if indexInfo, err = c.Info(); err == nil {
						logIndexInfo(indexInfo)
					}
				} else {
					errorLog.Printf("Error creating schema %+v:%s", schema, err)
				}
			}
		}
		ic.redisClients[ns] = c
	}
	worker := ic.workers[ns]
	if worker == nil {
		worker = &indexWorker{
			config:        ic.config,
			client:        c,
			mongoClient:   ic.mongoClient,
			indexers:      ic.indexers,
			namespace:     ns,
			maxDuration:   ic.maxDuration,
			retryDuration: ic.retryDuration,
			maxItems:      ic.maxItems,
			maxSize:       ic.maxSize,
			workQ:         make(chan *gtm.Op),
			stopC:         ic.stopC,
			allWg:         ic.allWg,
			logs:          ic.logs,
			stats:         ic.stats,
			createIndex:   ic.createIndex,
			schema:        schema,
		}
		ic.workers[ns] = worker
		worker.start()
	}
	if op.IsInsert() || op.IsUpdate() {
		worker.add(op)
	} else if op.IsDelete() {
		worker.remove(op)
	}
	ic.setTimestamp(op.Timestamp)
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
				errorLog.Printf("MongoDB connection failed: %s", err)
			}
		}
	}
	return client
}

func startReads(client *mongo.Client, conf *config) *gtm.OpCtx {
	ctx := gtm.Start(client, &gtm.Options{
		NamespaceFilter:    conf.nsFilter(),
		After:              conf.resumeFunc(),
		Pipe:               conf.buildPipe(),
		ChangeStreamNs:     conf.ChangeStreamNs,
		DirectReadNs:       conf.DirectReadNs,
		DirectReadConcur:   conf.DirectReadConcur,
		DirectReadSplitMax: int32(conf.DirectReadSplitMax),
		OpLogDisabled:      true,
	})
	return ctx
}

func main() {
	var (
		client *mongo.Client
		conf   *config
	)
	conf = mustConfig()
	if conf.viewConfig {
		conf.log(os.Stdout)
		return
	}
	infoLog.Println("Establishing connection to MongoDB")
	client = mustConnect(conf)
	infoLog.Println("Connected to MongoDB")
	defer client.Disconnect(context.Background())
	ctx := startReads(client, conf)
	indexClient := newIndexClient(client, ctx, conf)
	indexClient.eventLoop()
}
