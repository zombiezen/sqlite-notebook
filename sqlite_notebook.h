#define _XOPEN_SOURCE 600
#include <unistd.h>

#include <stddef.h>

struct sqliteNBSockets {
  void* controlSocket;
  void* shellSocket;
  void* ioPubSocket;
  void* stdinSocket;
  void* heartbeatSocket;

  char* signatureScheme;
  void* key;
  size_t keySize;
};

int sqliteNBLoadSockets(void* zmqCtx, struct sqliteNBSockets* sockets, const char* filePath);
void sqliteNBCloseSockets(struct sqliteNBSockets* sockets);
