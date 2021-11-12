package go_cache

import (
	"fmt"
	"go_cache/consistenthash"
	pb "go_cache/proto"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
)

const (
	defaultBasePath = "/_geecache/"
	defaultReplicas = 50
)

type NodeHandler struct {
	selfHost string //当前节点的地址, eg: "https://example.net:8000"
	basePath string
	mu       sync.Mutex
	hashRing *consistenthash.Map //实现了一致性哈希,用来计算 key 应该分配到哪个节点
	nodes    map[string]*Node    //关联的节点, eg "http://10.0.0.2:8008"
}

// NewNodeHandler initializes an HTTP pool of hashRing.
func NewNodeHandler(host string) *NodeHandler {
	return &NodeHandler{
		selfHost: host,
		basePath: defaultBasePath,
	}
}

// Log info with server name
func (h *NodeHandler) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", h.selfHost, fmt.Sprintf(format, v...))
}

// ServeHTTP handle all http requests
func (h *NodeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, h.basePath) {
		panic("unexpected urlPath: " + r.URL.Path)
	}
	h.Log("%s %s", r.Method, r.URL.Path)

	// /<basepath>/<namespace>/<key> required
	parts := strings.SplitN(r.URL.Path[len(h.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	namespace := parts[0]
	key := parts[1]

	cache := GetCache(namespace)
	if cache == nil {
		http.Error(w, "no such cache: "+namespace, http.StatusNotFound)
		return
	}

	val, err := cache.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	//使用 protobuf 编码数据
	body, err := proto.Marshal(&pb.Response{Value: val.Clone()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")

	w.Write(body)
}

// AddNode add node to hashRing.
func (h *NodeHandler) AddNode(hosts ...string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hashRing = consistenthash.New(defaultReplicas, nil)
	h.hashRing.Add(hosts...)
	h.nodes = make(map[string]*Node, len(hosts))
	for _, host := range hosts {
		h.nodes[host] = &Node{baseURL: host + h.basePath}
	}
}

// NodeSelect select a node according to key
func (h *NodeHandler) NodeSelect(key string) (*Node, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if host := h.hashRing.Get(key); host != "" && host != h.selfHost {
		h.Log("select node %s", host)
		return h.nodes[host], true
	}
	return nil, false
}

type Node struct {
	baseURL string
}

func (c *Node) Request(in *pb.Request, out *pb.Response) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		c.baseURL,
		url.QueryEscape(in.GetNamespace()),
		url.QueryEscape(in.GetKey()),
	)
	res, err := http.Get(u)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}

	if err = proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}

	return nil
}
