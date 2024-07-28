package proto

type Command int

const (
	UNKNOWN Command = iota
	PING
	GET
	SET
	DEL
	KEYS
)

func (c *Command) UnmarshalText(text []byte) error {
	switch string(text) {
	case PING.String():
		*c = PING
	case GET.String():
		*c = GET
	case SET.String():
		*c = SET
	case DEL.String():
		*c = DEL
	case KEYS.String():
		*c = KEYS
	default:
		*c = UNKNOWN
	}
	return nil
}

func (c Command) String() string {
	switch c {
	case PING:
		return "PING"
	case GET:
		return "GET"
	case SET:
		return "SET"
	case DEL:
		return "DEL"
	case KEYS:
		return "KEYS"
	default:
		return "UNKNOWN"
	}
}
