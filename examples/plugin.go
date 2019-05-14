package main

import (
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/rwynn/gtm"
	"github.com/rwynn/redisetgo/module"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// All plugins must assign the InitPlugin symbol to
// a type implementing the module.PluginIntializer interface
var InitPlugin myPlugin

type myPlugin struct{}

func (p *myPlugin) Get() module.Plugin {
	plugin := module.Plugin{}
	// you can opt in to setting Events, Schemas, and Pipelines
	// you don't need to set all in each plugin
	plugin.Events = []module.EventHandler{&pluginHandler{name: "examplePluginHandler"}}
	plugin.Schemas = &pluginSchema{name: "examplePluginSchema"}
	plugin.Pipeline = &pluginPipeline{name: "examplePluginPipeline"}
	return plugin
}

type pluginPipeline struct {
	name string
}

func (pipe *pluginPipeline) Name() string {
	return pipe.name
}

func (pipe *pluginPipeline) BuildPipeline(r *module.PipeRequest) (*module.PipeResponse, error) {
	// This pipeline puts a filter on direct reads and change events
	// such that only those documents with the foo property are processed
	var stages []bson.M
	if r.ChangeStream {
		// for a change stream the document is nested instead a fullDocument field
		stage := bson.M{
			"$match": bson.M{
				"fullDocument.foo": bson.M{"$exists": true},
			},
		}
		stages = append(stages, stage)
	} else {
		stage := bson.M{
			"$match": bson.M{
				"foo": bson.M{"$exists": true},
			},
		}
		stages = append(stages, stage)
	}
	return &module.PipeResponse{Stages: stages}, nil
}

type pluginSchema struct {
	name string
}

func (s *pluginSchema) Name() string {
	return s.name
}

func (s *pluginSchema) Handles(ev *module.SchemaEvent) bool {
	return ev.Namespace == "test.test"
}

func (s *pluginSchema) Schema(ev *module.SchemaEvent) (*module.SchemaResponse, error) {
	// Set the name of the RediSearch namespace to be my-index
	// instead of defaulting to the MongoDB namespace, which is in this case test.test
	// Then you can search with the command ft.search my-index *
	index := "my-index"
	// Create a schema for the index with one text field named foo
	schema := redisearch.NewSchema(redisearch.DefaultOptions)
	schema.AddField(redisearch.NewTextField("foo"))
	return &module.SchemaResponse{
		Schema: schema,
		Index:  index,
	}, nil
}

type pluginHandler struct {
	name string
}

func (h *pluginHandler) Name() string {
	return h.name
}

func (h *pluginHandler) Handles(ev *module.Event) bool {
	return ev.Op.Namespace == "test.test"
}

func (h *pluginHandler) handleUpsert(op *gtm.Op) (*module.IndexResponse, error) {
	// maps the MongoDB document to 1 or more corresponding RediSearch documents
	// in this case we map it 1 to 1 only projecting the foo field
	id := op.Id
	if docId, ok := id.(primitive.ObjectID); ok {
		doc := redisearch.NewDocument(docId.Hex(), 1.0)
		data := op.Data
		if data["foo"] == nil {
			return nil, fmt.Errorf("Expected document to contain a foo property")
		}
		if val, ok := data["foo"].(string); ok {
			doc.Set("foo", val)
			return &module.IndexResponse{
				ToIndex:  []redisearch.Document{doc},
				Finished: true,
			}, nil
		} else {
			return nil, fmt.Errorf("Document foo property was not a string")
		}
	} else {
		return nil, fmt.Errorf("Expected document _id to be an ObjectID")
	}
}

func (h *pluginHandler) OnInsert(ev *module.InsertEvent) (*module.IndexResponse, error) {
	// the event has access to the MongoDB and RediSearch clients for complex processing
	// in this case we only map the MongoDB document to a RediSearch document
	// remember that to suppress the default index handler one must return Finished:true
	// in the response
	return h.handleUpsert(ev.Op)
}

func (h *pluginHandler) OnUpdate(ev *module.UpdateEvent) (*module.IndexResponse, error) {
	// the event has access to the MongoDB and RediSearch clients for complex processing
	// in this case we only map the MongoDB document to a RediSearch document
	// remember that to suppress the default index handler one must return Finished:true
	// in the response
	return h.handleUpsert(ev.Op)
}

func (h *pluginHandler) OnDelete(ev *module.DeleteEvent) (*module.DeleteResponse, error) {
	id := ev.Op.Id
	if docId, ok := id.(primitive.ObjectID); ok {
		return &module.DeleteResponse{
			ToDelete: []string{docId.Hex()},
			Finished: true,
		}, nil
	} else {
		return nil, fmt.Errorf("Expected document _id to be an ObjectID")
	}
}
