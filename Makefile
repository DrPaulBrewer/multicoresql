CC=gcc
CFLAGS=-I. -fPIC
SRCDIR = ./src
BINDIR = ./bin
DEPS = libmulticoresql0.h libmulticoresql1.h libmulticoreutils.h
OBJ = libmulticoresql0.o libmulticoresql1.o libmulticoreutils.o q0.o q1.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

q0: q0.c libmulticoresql0.c libmulticoreutils.c
	gcc -o $@ $^ $(CFLAGS)

q1: q1.c libmulticoresql1.c libmulticoreutils.c
	gcc -o $@ $^ $(CFLAGS)

all: q0 q1

test1: test1.c libmulticoreutils.c
	gcc -o $@ $^ $(CFLAGS)

test2: test2.c libmulticoresql0.c libmulticoreutils.c
	gcc -o $@ $^ $(CFLAGS)

test3: test3.c libmulticoresql1.c libmulticoreutils.c
	gcc -o $@ $^ $(CFLAGS)

tests: test1
	./test1

