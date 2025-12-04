package bitcask

type UID int64

type Timestamps struct {
	Created int64
	Expired int64
}

type Reference struct {
	Address int64
	Size    int
}

type Record struct {
	Key []byte

	CID UID
	Ts  Timestamps
	Ref Reference
}

type Block struct {
	Sign uint64

	Ts Timestamps

	Key   []byte
	Value []byte
}
