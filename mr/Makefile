all: worker master

worker : worker.cpp
	@echo "Compile worker.cpp"
	g++ worker.cpp -o worker -lzmq -ldl -pthread

master : master.cpp
	@echo "Compile master.cpp"
	g++ master.cpp -o master -lzmq -pthread
