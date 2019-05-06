#include <stdio.h>
#include <unistd.h>
#include "getPassword.h"

char *passwordPrompt(void) {
  char *password;
  password = getpass("Enter MySQL Password: ");

  return password;
}
