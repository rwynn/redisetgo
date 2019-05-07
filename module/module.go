package module

import (
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/rwynn/gtm"
	"go.mongodb.org/mongo-driver/mongo"
    "log"
)

type PluginEntry func() []Plugin

type Loggers struct {
	InfoLog  *log.Logger
	WarnLog  *log.Logger
	TraceLog *log.Logger
	ErrorLog *log.Logger
}

type EventBase struct {
    MongoClient *mongo.Client
    RedisSearchClient *redisearch.Client
    Event *gtm.Op
    Logs *Loggers
}

type InsertEvent struct {
    EventBase
}

type UpdateEvent struct {
    EventBase
}

type DeleteEvent struct {
    EventBase
}

type IndexResponse struct {
    Finished bool
	ToIndex []redisearch.Document
}

type DeleteResponse struct {
    Finished bool
	ToDelete []string
}

type Plugin interface {
    OnInsert(*InsertEvent) (*IndexResponse, error)
    OnUpdate(*UpdateEvent) (*IndexResponse, error)
    OnDelete(*DeleteEvent) (*DeleteResponse, error)
}

