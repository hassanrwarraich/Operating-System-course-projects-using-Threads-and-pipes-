all: mvt
mvt: mvt.c
	gcc -g -Wall  -o mvt mvt.c -lpthread
clean: 
	rm -fr *~ mvt
run:
	./mvt m_file.txt v_file.txt out.txt 2
