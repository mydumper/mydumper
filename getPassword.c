#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include "getPassword.h"

char *passwordPrompt(void) {
  char *password;
  password = getpass("Enter MySQL Password: ");

  return password;
}
