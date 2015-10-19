CC=gcc
CFLAGS=-ansi -pedantic -Wall
EXE=simq
OFILES=simq.o

simq : $(OFILES)
	$(CC) -o $@ $<

.c.o :
	$(CC) $(CFLAGS) -c -o $@ $<

clean :
	\rm -f $(OFILES)

distclean : clean
	\rm -f $(EXE)
