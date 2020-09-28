package main

//go:generate ydbgen

//ydb:gen
type Project struct {
	id          uint64
	name        string
	description string
	creator     uint64
}

//ydb:gen scan,value
type Projects []Project
