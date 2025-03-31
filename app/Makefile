libmrFunc.so:mrapp.o
	@echo "Compile mrapp.cpp into the libmrFunc.so file and place it in the obj folder."
	@g++ -shared mrapp.o -o ../obj/libmrFunc.so
mrapp.o:mrapp.cpp
	@g++ -fPIC -c mrapp.cpp

clean:
	rm *.o