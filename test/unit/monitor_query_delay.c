/* Integration fault injection: block actual monitor I/O, not the client thread. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <string.h>

int mysql_query(void *conn, const char *query)
{
  int (*real_query)(void *, const char *) = dlsym(RTLD_NEXT, "mysql_query");
  if (!strcmp(query, "SHOW PROCESSLIST"))
  {
    int result = real_query(conn, "SELECT SLEEP(10)");
    if (result)
      return result;
    void *(*store)(void *) = dlsym(RTLD_NEXT, "mysql_store_result");
    void (*release)(void *) = dlsym(RTLD_NEXT, "mysql_free_result");
    void *rows = store(conn);
    if (rows)
      release(rows);
  }
  return real_query(conn, query);
}
