package go_cache

type Byte []byte

func (b Byte) Len() int {
	return len(b)
}

func (b Byte) String() string {
	return string(b)
}

func (b Byte) Clone() []byte  {
	c := make([]byte, b.Len())
	copy(c, b)
	return c
}
