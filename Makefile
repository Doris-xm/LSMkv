#
#LINK.o = $(LINK.cc)
#CXXFLAGS = -std=c++14 -Wall
#
#all: correctness persistence
#
#correctness: kvstore.o correctness.o
#
#persistence: kvstore.o persistence.o
#
#clean:
#	-rm -f correctness persistence *.o
CC = g++
CXXFLAGS = -std=c++14 -Wall
SRC_DIR = src
HEADER_DIR = header

all: correctness persistence

correctness: $(SRC_DIR)/kvstore.o $(SRC_DIR)/Disk_store.o $(SRC_DIR)/skiplist.o $(SRC_DIR)/SStable.o test/correctness.o
	$(CC) $(CXXFLAGS) -o correctness $(SRC_DIR)/kvstore.o $(SRC_DIR)/Disk_store.o $(SRC_DIR)/skiplist.o $(SRC_DIR)/SStable.o test/correctness.o

persistence: $(SRC_DIR)/kvstore.o $(SRC_DIR)/Disk_store.o $(SRC_DIR)/skiplist.o $(SRC_DIR)/SStable.o test/persistence.o
	$(CC) $(CXXFLAGS) -o persistence $(SRC_DIR)/kvstore.o $(SRC_DIR)/Disk_store.o $(SRC_DIR)/skiplist.o $(SRC_DIR)/SStable.o test/persistence.o

$(SRC_DIR)/kvstore.o: $(SRC_DIR)/kvstore.cpp $(HEADER_DIR)/kvstore.h
	$(CC) $(CXXFLAGS) -c $(SRC_DIR)/kvstore.cpp -o $(SRC_DIR)/kvstore.o

$(SRC_DIR)/Disk_store.o: $(SRC_DIR)/Disk_store.cpp $(HEADER_DIR)/Disk_store.h
	$(CC) $(CXXFLAGS) -c $(SRC_DIR)/Disk_store.cpp -o $(SRC_DIR)/Disk_store.o

$(SRC_DIR)/skiplist.o: $(SRC_DIR)/skiplist.cpp $(HEADER_DIR)/skiplist.h
	$(CC) $(CXXFLAGS) -c $(SRC_DIR)/skiplist.cpp -o $(SRC_DIR)/skiplist.o

$(SRC_DIR)/SStable.o: $(SRC_DIR)/SStable.cpp $(HEADER_DIR)/SStable.h
	$(CC) $(CXXFLAGS) -c $(SRC_DIR)/SStable.cpp -o $(SRC_DIR)/SStable.o


test/correctness.o: test/correctness.cpp
	$(CC) $(CXXFLAGS) -c test/correctness.cpp -o test/correctness.o

test/persistence.o: test/persistence.cpp
	$(CC) $(CXXFLAGS) -c test/persistence.cpp -o test/persistence.o

clean:
	-rm -f correctness persistence $(SRC_DIR)/kvstore.o $(SRC_DIR)/Disk_store.o $(SRC_DIR)/skiplist.o $(SRC_DIR)/SStable.o test/correctness.o test/persistence.o
