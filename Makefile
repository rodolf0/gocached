include $(GOROOT)/src/Make.inc

TARG = gocached

GOFILES = \
	gocached.go \
	mapstorage.go \
	hashingstorage.go \
	command.go \
	storage.go

include $(GOROOT)/src/Make.cmd
