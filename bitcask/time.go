package bitcask

import "time"

func unixTimestamp() int64 {
	return time.Now().UnixMilli()
}

func unixTimestampWithExpiration(dur time.Duration) (tstamp int64, exp int64) {
	now := time.Now()
	tstamp = now.UnixMilli()
	if dur != 0 {
		exp = now.Add(dur).UnixMilli()
	}
	return
}
