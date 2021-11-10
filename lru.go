package go_cache

import "container/list"

type Lru struct {
	memTotal  int64   //分配的缓存容量(byte)
	memUsed   int64   //已使用的缓存容量
	ll        *list.List  //双向链接模拟队列,方便移动元素位置
	data      map[string]*list.Element  //用来保存键和该键所在节点的指针,这样就很方便的知道某个key是否已经存在于链表中
	OnDelete func(key string, value Value)  //删除key时的回调
}

//双向链表节点的数据类型
type entry struct {
	key   string
	value Value
}

// Value use Len to count how many bytes it takes
type Value interface {
	Len() int
}

// NewLru is the Constructor of Lru
func NewLru(memTotal int64, onDelete func(string, Value)) *Lru {
	return &Lru{
		memTotal: memTotal,
		ll:       list.New(),
		data:     make(map[string]*list.Element),
		OnDelete: onDelete,
	}
}

// Set adds a value to the data.
func (lru *Lru) Set(key string, value Value) {

	//要添加的key已经存在于链表中,则通过 ele 获取该节点
	if ele, ok := lru.data[key]; ok {

		//将被放的的key移到队尾(双向链表作为队列，队首队尾是相对的，在这里约定 front 为队尾)
		lru.ll.MoveToFront(ele)

		//获取节点数据
		kv := ele.Value.(*entry)

		//统计缓存使用量
		lru.memUsed += int64(value.Len()) - int64(kv.value.Len())

		//更新数据
		kv.value = value
	} else {
		// 要添加的 key 不在链表中, 则直接在队尾添加该数据(节点的数据类型是 entry)
		ele := lru.ll.PushFront(&entry{key, value})

		//在 data 中记录该key所在的节点
		lru.data[key] = ele

		//统计缓存使用量
		lru.memUsed += int64(len(key)) + int64(value.Len())
	}

	//一旦超过缓存容量,则进行缓存淘汰
	for lru.memTotal != 0 && lru.memTotal < lru.memUsed {
		lru.RemoveOldest()
	}
}

// Get look ups a key's value
func (lru *Lru) Get(key string) (value Value, ok bool) {
	//先获取 key 所在的节点, 然后通过节点拿到数据
	if ele, ok := lru.data[key]; ok {
		lru.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		return kv.value, true
	}
	return
}

// RemoveOldest removes the oldest item
func (lru *Lru) RemoveOldest() {
	//获取队首节点
	ele := lru.ll.Back()

	if ele != nil {
		//链表中移除该节点
		lru.ll.Remove(ele)

		//该节点的数据
		kv := ele.Value.(*entry)

		//从 data 中删除 key
		delete(lru.data, kv.key)

		lru.memUsed -= int64(len(kv.key)) + int64(kv.value.Len())

		//如果用户注册了删除缓存的回调,则调用该函数
		if lru.OnDelete != nil {
			lru.OnDelete(kv.key, kv.value)
		}
	}
}

// Len the number of data entries
func (lru *Lru) Len() int {
	return lru.ll.Len()
}
