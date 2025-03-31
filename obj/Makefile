all : ECHO test
	@echo "File remove"
	rm -f *.o
test : mrapp.o
	@echo "Link the .o files"
	g++ mrapp.o -o ../test
ECHO :
	@echo "In directory obj"
