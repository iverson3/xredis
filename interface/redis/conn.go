package redis

type Connection interface {
	Write([]byte) error
	SetPassword(string)
	GetPassword() string

	// used for multi database
	GetDBIndex() int
	SelectDB(int)
}
