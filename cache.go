package go_cache

import (
	"fmt"
	"sync"
)

//DataSource 定义了数据源, 在缓存不存在时, 调用Get这个函数得到源数据
type DataSource interface {
	Get(key string) ([]byte, error)
}

//DataFunc 实现了DataSource,我们称其为【接口型函数】
type DataFunc func(key string) ([]byte, error)

func (f DataFunc) Get(key string) ([]byte, error) {
	return f(key)
}

var (
	caches = make(map[string]*Cache)
)

//Cache 实现了并发安全的读取缓存
type Cache struct {
	namespace  string //缓存的命名空间, 比如学生和动物都有年龄,但一个 age 字段无法存储两个值,因此就需要命名空间来划分这两个 age
	lru        *Lru
	datasource DataSource
	mu         sync.RWMutex
}

//NewCache 实例化 Cache
func NewCache(namespace string, bytesTotal int64, datasource DataSource) *Cache {
	c := &Cache{
		namespace:  namespace,
		lru:        NewLru(bytesTotal, nil),
		datasource: datasource,
	}
	caches[namespace] = c
	return c
}

//GetCache 返回某个命名空间下的cache
func GetCache(namespace string) *Cache {
	return caches[namespace]
}

//Set 写入缓存数据
func (c *Cache) Set(key string, value Byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Set(key, value)
}

//Get 返回缓存数据
func (c *Cache) Get(key string) (Byte, error) {
	if key == "" {
		return nil, fmt.Errorf("key is required")
	}

	//从缓存中获取
	value, ok := c.read(key)
	if ok {
		return value, nil
	}

	//从数据源获取
	return c.getSource(key)
}

//read 返回 lru 中的数据
func (c *Cache) read(key string) (Byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.lru.Get(key)

	if ! ok {
		return Byte{}, false
	}

	return v.(Byte), ok
}

//getSource 返回源数据
func (c *Cache) getSource(key string) (Byte, error) {
	//调用用户注册的数据获取函数
	bytes, err := c.datasource.Get(key)
	if err != nil {
		return nil, err
	}

	//为了防止返回后的数据被篡改,这里克隆一份数据后返回
	value := Byte(bytes).Clone()

	//写入缓存
	c.Set(key, value)

	return value, nil
}
