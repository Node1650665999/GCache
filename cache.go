package go_cache

import (
	"fmt"
	pb "go_cache/proto"
	"sync"
	"time"
)

//DataSource 定义了数据源, 在缓存不存在时, 调用这个函数得到源数据
type DataSource interface {
	Get(key string) ([]byte, error)
}

//DataFunc 实现了DataSource,我们称其为【接口型函数】
type DataFunc func(key string) ([]byte, error)

func (f DataFunc) Get(key string) ([]byte, error) {
	return f(key)
}

var (
	mux    sync.RWMutex
	caches = make(map[string]*Cache)
)

// call is an in-flight or completed Do call
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

//Cache 实现了并发安全的读取缓存
type Cache struct {
	namespace   string //缓存的命名空间, 比如学生和动物都有年龄,但一个 age 字段无法存储两个值,因此就需要命名空间来划分这两个 age
	lru         *Lru
	datasource  DataSource
	mu          sync.RWMutex
	nodeHandler *NodeHandler
	remoteNode  string
	m           map[string]*call // lazily initialized
}

//Do 保证fn只被调用一次
func (cache *Cache) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	cache.mu.Lock()
	if cache.m == nil {
		cache.m = make(map[string]*call)
	}

	//检测到 fn 已经在执行了
	if c, ok := cache.m[key]; ok {
		cache.mu.Unlock()  //释放抢占的锁
		c.wg.Wait()        //阻塞等待直到fn返回结果(即wg.Down())
		return c.val, c.err
	}

	//添加标志位,标识 fn 已经在执行
	c := new(call)
	c.wg.Add(1)
	cache.m[key] = c
	cache.mu.Unlock()  //释放抢占的锁, 这样其他进程拿到锁后进入被 wg.Wait() 阻塞的状态

	//fn 结果返回后,调用 wg.Done(), 这样被 wg.Wait() 阻塞的代码得以执行
	c.val, c.err = fn()
	c.wg.Done()

	//删除标志位
	//	注意：删除有可能出现缓存穿透的情况,因为有其他的进程可能还等着获取锁,
	//	因此 sleep 个几秒后等待其他并发进程返回后再删除标志位,这样就不会有漏网之鱼
	time.Sleep(50 * time.Millisecond)
	cache.mu.Lock()
	delete(cache.m, key)
	cache.mu.Unlock()

	return c.val, c.err
}

//NewCache 实例化 Cache
func NewCache(namespace string, bytesTotal int64, datasource DataSource) *Cache {
	mux.Lock()
	defer mux.Unlock()
	c := &Cache{
		namespace:  namespace,
		lru:        NewLru(bytesTotal, nil),
		datasource: datasource,
	}
	caches[namespace] = c
	return c
}

//CacheObject 返回某个命名空间下的 cache 对象
func CacheObject(namespace string) *Cache {
	mux.Lock()
	defer mux.Unlock()
	return caches[namespace]
}

func (cache *Cache) SetRemoteNode(remoteNode string) {
	cache.remoteNode = remoteNode
}

func (cache *Cache) GetRemoteNode() string {
	return cache.remoteNode
}

//SetNodeHandler set a nodeHandler for select remote node
func (cache *Cache) SetNodeHandler(nodeHandler *NodeHandler) {
	if cache.nodeHandler == nil {
		cache.nodeHandler = nodeHandler
	}
}

//Set 写入数据
func (cache *Cache) Set(key string, value Byte) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.lru.Set(key, value)
}

//Get 返回数据
func (cache *Cache) Get(key string) (Byte, error) {
	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	//从缓存中获取
	value, ok := cache.getCache(key)
	if ok {
		return value, nil
	}

	viewi, err := cache.Do(key, func() (interface{}, error) {
		//从远程节点获取
		value, err := cache.getRemote(key)
		if err == nil {
			return value, nil
		}

		//从本地获取
		return cache.getLocal(key)
	})

	if err == nil {
		return viewi.(Byte), nil
	}
	return Byte{}, nil
}

//getCache get key from cache
func (cache *Cache) getCache(key string) (Byte, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	v, ok := cache.lru.Get(key)

	if !ok {
		return Byte{}, false
	}

	return v.(Byte), ok
}

//getRemote get key from remote
func (cache *Cache) getRemote(key string) (Byte, error) {
	if cache.nodeHandler == nil {
		return nil, fmt.Errorf("nodeHandler is nil")
	}

	node, ok := cache.nodeHandler.NodeSelect(key)
	if !ok {
		return nil, fmt.Errorf("select node faild, key=%s", key)
	}

	cache.SetRemoteNode(node.host)

	req := &pb.Request{
		Namespace: cache.namespace,
		Key:       key,
	}
	res := &pb.Response{}
	err := node.Request(req, res)
	if err != nil {
		return nil, err
	}

	return Byte(res.Value), nil
}

//getLocal get key from local
func (cache *Cache) getLocal(key string) (Byte, error) {
	//源数据获取
	bytes, err := cache.datasource.Get(key)
	if err != nil {
		return nil, err
	}

	//为了防止返回后的数据被篡改,这里克隆一份数据后返回
	value := Byte(bytes).Clone()

	//写入缓存
	cache.Set(key, value)

	return value, nil
}
