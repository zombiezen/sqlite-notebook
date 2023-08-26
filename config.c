#include "sqlite_notebook.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sqlite3.h>
#include <zmq.h>

static ssize_t readFile(const char* path, char** buf) {
  FILE* f = fopen(path, "r");
  if (f == NULL) {
    return -1;
  }

  size_t n = 0;
  size_t cap = 4096;
  *buf = calloc(cap, sizeof(char));
  for (;;) {
    size_t nread = fread((*buf)+n, sizeof(char), cap-n, f);
    n += nread;
    if (nread == 0) {
      break;
    }
    if (n == cap) {
      cap *= 2;
      *buf = realloc(*buf, cap * sizeof(char));
    }
  }
  fclose(f);
  return n;
}

static void copyColumnText(char** dst, sqlite3_stmt* stmt, int col) {
  const unsigned char* ptr = sqlite3_column_text(stmt, col);
  size_t n = sqlite3_column_bytes(stmt, col) + 1;
  *dst = calloc(n, sizeof(char));
  memcpy(*dst, ptr, n);
}

struct kernelConfiguration {
  char* transport;
  char* ip;
  int controlPort;
  int shellPort;
  int ioPubPort;
  int stdinPort;
  int heartbeatPort;

  char* signatureScheme;
  unsigned char* key;
  size_t keySize;
};

static int parseKernelConfiguration(struct kernelConfiguration* config, const char* configJSON, size_t configJSONSize) {
  memset(config, 0, sizeof(struct kernelConfiguration));

  sqlite3* db;
  int rc = sqlite3_open(":memory:", &db);
  if (rc != SQLITE_OK) {
    sqlite3_close(db);
    errno = ENOMEM;
    return -1;
  }

  sqlite3_stmt* stmt;
  rc = sqlite3_prepare_v2(db, "values (?1 ->> 'transport', ?1 ->> 'ip', ?1 ->> 'control_port', ?1 ->> 'shell_port', ?1 ->> 'iopub_port', ?1 ->> 'stdin_port', ?1 ->> 'hb_port', ?1 ->> 'signature_scheme', unhex(?1 ->> 'key'));", -1, &stmt, NULL);
  if (rc != SQLITE_OK) {
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    errno = EIO;
    return -1;
  }
  rc = sqlite3_bind_blob(stmt, 1, configJSON, configJSONSize, SQLITE_STATIC);
  if (rc != SQLITE_OK) {
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    errno = EIO;
    return -1;
  }
  if (sqlite3_step(stmt) != SQLITE_ROW) {
    fprintf(stderr, "sqlite-notebook: parse json: %s\n", sqlite3_errmsg(db));
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    errno = EINVAL;
    return -1;
  }

  copyColumnText(&config->transport, stmt, 0);
  copyColumnText(&config->ip, stmt, 1);
  config->controlPort = sqlite3_column_int(stmt, 2);
  config->shellPort = sqlite3_column_int(stmt, 3);
  config->ioPubPort = sqlite3_column_int(stmt, 4);
  config->stdinPort = sqlite3_column_int(stmt, 5);
  config->heartbeatPort = sqlite3_column_int(stmt, 6);
  copyColumnText(&config->signatureScheme, stmt, 7);
  const void* key = sqlite3_column_blob(stmt, 8);
  config->keySize = sqlite3_column_bytes(stmt, 8);
  config->key = malloc(config->keySize);
  memcpy(config->key, key, config->keySize);

  sqlite3_finalize(stmt);
  sqlite3_close(db);
  return 0;
}

static void destroyKernelConfiguration(struct kernelConfiguration* config) {
  if (config->transport != NULL) {
    free(config->transport);
    config->transport = NULL;
  }
  if (config->ip != NULL) {
    free(config->ip);
    config->ip = NULL;
  }
  if (config->signatureScheme != NULL) {
    free(config->signatureScheme);
    config->signatureScheme = NULL;
  }
  if (config->key != NULL) {
    free(config->key);
    config->key = NULL;
    config->keySize = 0;
  }
}

static void* openSocket(void* zmqCtx, int typ, const char* transport, const char* ip, int port) {
  void* sock = zmq_socket(zmqCtx, typ);
  if (sock == NULL) {
    return NULL;
  }
  char endpoint[1024];
  snprintf(endpoint, sizeof(endpoint)/sizeof(char), "%s://%s:%d", transport, ip, port);
  if (zmq_bind(sock, endpoint)) {
    zmq_close(sock);
    return NULL;
  }
  return sock;
}

int sqliteNBLoadSockets(void* zmqCtx, struct sqliteNBSockets* sockets, const char* filePath) {
  memset(sockets, 0, sizeof(struct sqliteNBSockets));

  char* configJSON;
  ssize_t configJSONSize = readFile(filePath, &configJSON);
  if (configJSONSize < 0) {
    return -1;
  }

  struct kernelConfiguration cfg;
  if (parseKernelConfiguration(&cfg, configJSON, configJSONSize)) {
    return -1;
  }

  sockets->controlSocket = openSocket(zmqCtx, ZMQ_ROUTER, cfg.transport, cfg.ip, cfg.controlPort);
  if (sockets->controlSocket == NULL) {
    sqliteNBCloseSockets(sockets);
    destroyKernelConfiguration(&cfg);
    return -1;
  }
  sockets->shellSocket = openSocket(zmqCtx, ZMQ_ROUTER, cfg.transport, cfg.ip, cfg.shellPort);
  if (sockets->shellSocket == NULL) {
    sqliteNBCloseSockets(sockets);
    destroyKernelConfiguration(&cfg);
    return -1;
  }
  sockets->ioPubSocket = openSocket(zmqCtx, ZMQ_PUB, cfg.transport, cfg.ip, cfg.ioPubPort);
  if (sockets->ioPubSocket == NULL) {
    sqliteNBCloseSockets(sockets);
    destroyKernelConfiguration(&cfg);
    return -1;
  }
  sockets->stdinSocket = openSocket(zmqCtx, ZMQ_ROUTER, cfg.transport, cfg.ip, cfg.stdinPort);
  if (sockets->stdinSocket == NULL) {
    sqliteNBCloseSockets(sockets);
    destroyKernelConfiguration(&cfg);
    return -1;
  }
  sockets->heartbeatSocket = openSocket(zmqCtx, ZMQ_REP, cfg.transport, cfg.ip, cfg.heartbeatPort);
  if (sockets->heartbeatSocket == NULL) {
    sqliteNBCloseSockets(sockets);
    destroyKernelConfiguration(&cfg);
    return -1;
  }

  sockets->signatureScheme = cfg.signatureScheme;
  sockets->key = cfg.key;
  sockets->keySize = cfg.keySize;
  cfg.signatureScheme = NULL;
  cfg.key = NULL;
  destroyKernelConfiguration(&cfg);
  return 0;
}

void sqliteNBCloseSockets(struct sqliteNBSockets* sockets) {
  if (sockets->key != NULL) {
    free(sockets->key);
    sockets->key = NULL;
    sockets->keySize = 0;
  }
  if (sockets->signatureScheme != NULL) {
    free(sockets->signatureScheme);
    sockets->signatureScheme = NULL;
  }
  if (sockets->heartbeatSocket) {
    zmq_close(sockets->heartbeatSocket);
    sockets->heartbeatSocket = NULL;
  }
  if (sockets->stdinSocket) {
    zmq_close(sockets->stdinSocket);
    sockets->stdinSocket = NULL;
  }
  if (sockets->ioPubSocket) {
    zmq_close(sockets->ioPubSocket);
    sockets->ioPubSocket = NULL;
  }
  if (sockets->shellSocket) {
    zmq_close(sockets->shellSocket);
    sockets->shellSocket = NULL;
  }
  if (sockets->controlSocket) {
    zmq_close(sockets->controlSocket);
    sockets->controlSocket = NULL;
  }
}
