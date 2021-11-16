package main

/*
$ curl "http://localhost:9999/api?key=Tom"
630
$ curl "http://localhost:9999/api?key=kkk"
kkk not exist
*/

import (
	"flag"
	"fmt"
	gocache "go_cache"
	"log"
	"net/http"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func Cache() *gocache.Cache {
	return gocache.NewCache("scores", 2<<10, gocache.DataFunc(
		func(key string) ([]byte, error) {
			//log.Printf("[From Local] search key : %s", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))
}

func startCacheServer(addr string, nodes []string, cache *gocache.Cache) {
	nodeHandler := gocache.NewNodeHandler(addr)
	nodeHandler.AddNode(nodes...)
	cache.SetNodeHandler(nodeHandler)
	log.Println("cache-server is running at", addr)
	log.Fatal(http.ListenAndServe(addr[7:], nodeHandler))
}

func startAPIServer(apiAddr string, cache *gocache.Cache) {
	http.Handle("/api", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Query().Get("key")
			val, err := cache.Get(key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write([]byte(string(val.Clone()) + "\n"))
		}))
	log.Println("api-server is running at", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr[7:], nil))

}

func main() {
	var port int
	var api bool
	flag.IntVar(&port, "port", 8001, "cache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.Parse()

	addrs := map[int]string{
		8001: "http://localhost:8001",
		8002: "http://localhost:8002",
		8003: "http://localhost:8003",
	}
	var nodes []string
	for _, v := range addrs {
		nodes = append(nodes, v)
	}

	//Cache 作为上下文传给 CacheServer 和 ApiServer
	cache := Cache()
	if api {
		apiAddr := "http://localhost:9999"
		go startAPIServer(apiAddr, cache)
	}

	startCacheServer(addrs[port], nodes, cache)
}