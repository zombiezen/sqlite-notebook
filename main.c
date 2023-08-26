#include "sqlite_notebook.h"

#include <stdlib.h>
#include <sysexits.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sqlite3.h>
#include <zmq.h>

int main(int argc, char *argv[]) {
  if (argc != 2) {
    fprintf(stderr, "usage: sqlite-notebook CONFIG\n");
    return EX_USAGE;
  }
  struct sqliteNBSockets sockets;
  int exitCode = 0;
  void* ctx = zmq_ctx_new();
  if (sqliteNBLoadSockets(ctx, &sockets, argv[1]) != 0) {
    char errbuf[1024];
    strerror_r(errno, errbuf, sizeof(errbuf));
    fprintf(stderr, "sqlite-notebook: unable to read kernel configuration: %s\n", errbuf);
    exitCode = 1;
    goto cleanup;
  }

  printf("signature scheme: %s\n", sockets.signatureScheme);

cleanup:
  sqliteNBCloseSockets(&sockets);
  zmq_ctx_term(ctx);
  return exitCode;
}
