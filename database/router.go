package database

import "strings"

var cmdTable = make(map[string]*command)

type command struct {
	executor ExecFunc
	prepare  PreFunc
	undo     UndoFunc
	arity    int // allow number of args, arity < 0 means len(args) >= -arity
	flags    int
}

func RegisterCommand(name string, executor ExecFunc, prepare PreFunc, rollback UndoFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		prepare:  prepare,
		undo:     rollback,
		arity:    arity,
	}
}
