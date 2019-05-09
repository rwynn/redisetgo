# redisetgo

Continuously index your MongoDB data stream in [RediSearch](https://redislabs.com/redis-enterprise/technology/redis-search/).

### testing with docker

Start up a redisearch server

```
docker run --rm -p 6379:6379 redislabs/redisearch:latest
```

Start up a redis client to inspect indexes

```
docker run --rm -it --network host redislabs/redisearch redis-cli
```

Start up the redisetgo daemon

```
go run redisetgo.go 
INFO 2019/05/01 14:33:31 Watching changes on the deployment
```

Create a document in MongoDB

```
rs1:PRIMARY> use test;
switched to db test
rs1:PRIMARY> db.test.insert({foo: 1})
```

Run a query via redis-cli to ensure the indexing was successful

```
127.0.0.1:6379> FT.SEARCH test.test *
1) (integer) 1
2) "5cc9ae490862227ef20c3116"
3) 1) "foo"
   2) "1"
```

### customize the behavior with modules

You can write you own modules to change the behavior.  See examples/plugin.go for an example.

To build and run the example plugin,

```
cd examples
go build -buildmode=plugin
cd ..
go run redisetgo.go -plugin examples/examples.so
```

In this case we have just one plugin but you can have multiple

```
go run redisetgo.go -plugin myplugins/p1.so -plugin myplugins/p2.so

```

Test the example plugin by inserting some data into MongoDB

```
rs1:PRIMARY> use test;
switched to db test
rs1:PRIMARY> db.test.insert({foo: "ok", bar: "ignored"}) // succeeds
rs1:PRIMARY> db.test.insert({foo: 1}) // fails as designed
```

Search for the data in RediSearch

```
127.0.0.1:6379> FT.SEARCH my-index *
1) (integer) 1
2) "5cd3131ba169676422407f1a"
3) 1) "foo"
   2) "ok"
```
