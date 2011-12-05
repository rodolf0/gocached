include $(GOROOT)/src/Make.inc

TARG = gocached

GOFILES = \
	gocached.go \
	command.go

include $(GOROOT)/src/Make.cmd
