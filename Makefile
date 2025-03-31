all : ECHO
	make -C app && make -C obj
ECHO :
	@echo "In main directory"
