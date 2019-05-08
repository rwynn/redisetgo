package main

import (
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/rwynn/gtm"
	"github.com/rwynn/redisetgo/module"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// All plugins must assign the InitPlugin symbol to
// a type implementing the module.PluginIntializer interface
var InitPlugin myPlugin

type myPlugin struct{}

func (p *myPlugin) Get() module.Plugin {
	plugin := module.Plugin{}
	plugin.Events = []module.EventHandler{&pluginHandler{name: "examplePluginHandler"}}
	plugin.Schemas = &pluginSchema{name: "examplePluginSchema"}
	return plugin
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
	index := "my-index"
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
	id := op.Id
	if docId, ok := id.(primitive.ObjectID); ok {
		doc := redisearch.NewDocument(docId.Hex(), 1.0)
		data := op.Data
		if data["foo"] == nil {
			return nil, fmt.Errorf("Expected doc to contain a foo property")
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
		return nil, fmt.Errorf("Expected ID to be an ObjectID")
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
		return nil, fmt.Errorf("Expected ID to be an ObjectID")
	}
}
