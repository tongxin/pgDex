
INCLUDE_SERVER := $(shell pg_config --includedir-server)
LIB := $(shell pg_config --libdir)

objects = fun.o clusterHost.o common.o dex.o dexConfiguration.o \
	dexConnection.o dexDataFrame.o dexDataModel.o \
	dexProtocol.o dexSession.o MsgType.o network.o

all:fun.so

fun.so:$(objects)
	gcc -shared -fpic -o fun.so $(objects)  -lzmq

$(objects):%.o:%.c
	gcc -I$(INCLUDE_SERVER)  -fpic -c $< -o $@


clean:
	rm -f *.o | rm -f fun.so
