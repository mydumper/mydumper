#include "getPassword.h"
#include <stdio.h>
#include <unistd.h>

char *passwordPrompt(void) {
  char *password;
  password = getpass("Enter MySQL Password: ");

  return password;
}
