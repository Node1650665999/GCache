package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

//Hash 定义了 hash 函数
type Hash func(data []byte) uint32

// Map constains all hashed nodes
type Map struct {
	hash     Hash
	replicas int            //每个节点的副本数量
	nodes    []int          //存储哈希环上的所有节点(真实节点和虚拟节点)
	hashMap  map[int]string //虚拟节点与真实节点的映射表
}

// New creates a Map instance
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}


//Add 添加节点到hash环上
func (m *Map) Add(nodes ...string) {

	//遍历真实节点
	for _, node := range nodes {

		//对每一个真实节点 node, 对应创建 m.replicas 个虚拟节点
		for i := 0; i < m.replicas; i++ {

			//通过为真实节点添加编号的方式构造出虚拟节点
			//例如真实节点 node=2, 添加编号后得到虚拟节点名称: 02 12 22
			hash := int(m.hash([]byte(strconv.Itoa(i) + node)))

			//将节点的hash值添加到环上
			m.nodes = append(m.nodes, hash)

			//多个虚拟节点映射同一个真实节点,键是虚拟节点的哈希值，值是真实节点的名称
			m.hashMap[hash] = node
		}
	}

	//环上的哈希值排序
	sort.Ints(m.nodes)
}

//Get 获取离key最近的那个节点
func (m *Map) Get(key string) string {
	//哈希环为空
	if len(m.nodes) == 0 {
		return ""
	}

	//获取 key 的哈希值
	hash := int(m.hash([]byte(key)))

	//顺时针找到第一个匹配的虚拟节点的索引
	idx := sort.Search(len(m.nodes), func(i int) bool {
		return m.nodes[i] >= hash
	})

	//m.nodes[idx] : 从环上获取虚拟节点;
	//不直接通过 idx 而是通过 idx%len(m.nodes) 来获取的原因:
	//由于 m.nodes 是一个环状结构, 如果 idx == len(m.nodes), 说明应选择 m.nodes[0](结尾即开头), 所以用取余数的方式来处理这种情况。
	//m.hashMap[xxx] : 映射得到真实的节点
	return m.hashMap[m.nodes[idx%len(m.nodes)]]
}