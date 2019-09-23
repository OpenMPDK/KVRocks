CFLAGS= -g -Wall -D_GNU_SOURCE -m64 -fPIC
LDFLAGS= -lm
KVLIB=libkvnvme.a
KVLIB_SOURCE=kv_nvme.c
KVLIB_OBJ=$(KVLIB_SOURCE:.c=.o)
KV_BENCH_SRC=kv_bench.c kv_worker.c
KV_BENCH=kv_bench

AR=ar
CC=gcc

all : $(KVLIB) $(KV_BENCH) 

$(KVLIB): $(KVLIB_SOURCE)
	$(CC) $(CFLAGS) -c $^
	$(AR) -cvq $@ $(KVLIB_OBJ)

$(KV_BENCH): $(KV_BENCH_SRC)
	$(CC) $(CFLAGS) -pthread $^ -o $@ $(KVLIB_OBJ) $(LDFLAGS)
clean:
	-rm -f $(KVLIB) $(KV_BENCH) *.o *~
