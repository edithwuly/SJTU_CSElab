#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <map>
#include <set>

#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

class lock_server_cache {
 private:
  int nacquire;

  enum lock_server_statu {UNLOCKED, LOCKED};

  struct lock_statu
  {
    std::string id;
    std::string retry;
    std::set<std::string> queue;
    lock_server_statu statu;
  };

  std::map<lock_protocol::lockid_t, lock_statu> lock_status;

  pthread_mutex_t mutex;
  pthread_cond_t cond;
 public:

  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
