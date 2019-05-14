package module

import (
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/rwynn/gtm"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
)

// PluginInitializer is the interface your plugin
// must expose as a var named "InitPlugin" to get
// initialized
type PluginInitializer interface {
	Get() Plugin
}

// Plugin is the type returned by the intializer
// function.  It contains a slice of event handlers
// and a schema handler. The event handlers respond
// to create, update, and delete events in MongoDB.
// The schema handlers are invoked when a MongoDB
// namespace event requires building a new index with
// schema in RediSearch.
type Plugin struct {
	Events   []EventHandler
	Schemas  SchemaHandler
	Pipeline PipeBuilder
}

// Loggers is the type holding a set of loggers available
// to event handlers
type Loggers struct {
	InfoLog  *log.Logger
	WarnLog  *log.Logger
	TraceLog *log.Logger
	ErrorLog *log.Logger
}

// EventBase is the type holding the common context available
// to all event handlers via the passed in event
type EventBase struct {
	MongoClient       *mongo.Client
	RedisSearchClient *redisearch.Client
	Op                *gtm.Op
	Logs              *Loggers
}

// Event is the type available to event handlers when determining
// whether or not the handler will handle the event
type Event struct {
	Op *gtm.Op
}

// InsertEvent is the type of event passed to the OnInsert function
// of the event handler
type InsertEvent struct {
	EventBase
}

// UpdateEvent is the type of event passed to the OnUpdate function
// of the event handler
type UpdateEvent struct {
	EventBase
}

// DeleteEvent is the type of event passed to the OnDelete function
// of the event handler
type DeleteEvent struct {
	EventBase
}

// SchemaEvent is the type of event passed to schema handlers. The
// event contains the namespace, i.e. db.collection, of the MongoDB
// event that occurred.  Schema handlers can use the namespace to
// return information about the corresponding RediSearch index to use
// and the schema to use when creating the RediSearch index.
type SchemaEvent struct {
	Namespace string
}

// IndexResponse is the type returned for insert and update events.
// The response indicates which if any RediSearch documents to index
// in response to the event.  It also indicates if the event handling
// should continue to the next handler or is finished.
type IndexResponse struct {
	Finished bool
	ToIndex  []redisearch.Document
}

// DeleteResponse is the type returned for delete events.
// The response indicates which if any RediSearch documents to delete
// in response to the event.  It also indicates if the event handling
// should continue to the next handler or is finished.
type DeleteResponse struct {
	Finished bool
	ToDelete []string
}

type SchemaResponse struct {
	Schema *redisearch.Schema
	Index  string
}

type SchemaHandler interface {
	Name() string
	Handles(*SchemaEvent) bool
	Schema(*SchemaEvent) (*SchemaResponse, error)
}

type EventHandler interface {
	Name() string
	Handles(*Event) bool
	OnInsert(*InsertEvent) (*IndexResponse, error)
	OnUpdate(*UpdateEvent) (*IndexResponse, error)
	OnDelete(*DeleteEvent) (*DeleteResponse, error)
}

type PipeRequest struct {
	Namespace    string
	ChangeStream bool
}

type PipeResponse struct {
	Stages []bson.M
}

type PipeBuilder interface {
	Name() string
	BuildPipeline(*PipeRequest) (*PipeResponse, error)
}
