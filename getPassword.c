#include <stdio.h>
#include <stdlib.h>
#include "getPassword.h"

void* safeRealloc(void* ptr, size_t size) {
	void *newPtr = realloc(ptr,size);
	
	//if out of memory
	if(newPtr == NULL){
		// the memory block at ptr is not deallocated by realloc
		free(ptr);
	}
	return newPtr;
}

char* allocFromStdin(void){
	//initial str size to store input
	int size = 4;
	
	char* str = malloc(size*sizeof(char));
	if(str == NULL){
		//out of memory
		return NULL;
	}
	char c = '\0';
	int i=0;
	do {
		c = getchar();
		if(c == '\r' || c == '\n') {
			// end str if user hits enter
			c = '\0';
		}
		if(i == size) {
			// duplicate str size
			size *=2;
			//reallocate it
			str = safeRealloc(str, size*sizeof(char));
			if (str == NULL) {
				//out of memory
				return NULL;
			}
		}
		str[i++] = c;
	} while (c != '\0');
	//trim memory to the str content size
	str = safeRealloc(str, i);
	return str;
}

char* passwordPrompt(void) {
	puts("Enter Your MySQL Password:");
	char *password = allocFromStdin();

	return password;
}
