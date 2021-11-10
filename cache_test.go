package go_cache

import (
	"fmt"
	"testing"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}


var loadCounts = make(map[string]int, len(db))

//自定义源数据获取函数
var fn DataFunc = func(key string) ([]byte, error) {
	if v, ok := db[key]; ok {
		if _, ok := loadCounts[key]; !ok {
			loadCounts[key] = 0
		}
		loadCounts[key] += 1
		return []byte(v), nil
	}
	return nil, fmt.Errorf("%s not exist", key)
}


func TestCacheGet(t *testing.T) {
	gee := NewCache("scores", 2<<10, fn)

	for k, v := range db {
		//第一次从数据源中读取
		if view, err := gee.Get(k); err != nil || view.String() != v {
			t.Fatal("failed to get value of Tom")
		}
		//第二次从缓存中的读取
		if _, err := gee.Get(k); err != nil || loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		}
	}

	if view, err := gee.Get("unknown"); err == nil {
		t.Fatalf("the value of unknow should be empty, but %s got", view)
	}

}
