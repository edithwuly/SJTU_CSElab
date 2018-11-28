// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&mutex);
  int ret = lock_protocol::OK;
  if (lock_status.find(lid) == lock_status.end())
  {
    lock_statu temp;
    temp.statu = NONE;
    temp.revoke = false;
    temp.retry = false;
    pthread_cond_init(&temp.incond, NULL);
    pthread_cond_init(&temp.outcond, NULL);

    lock_status[lid] = temp;
  }

  while (lock_status[lid].statu == LOCKED || lock_status[lid].statu == ACQUIRING || lock_status[lid].statu == RELEASING)
    pthread_cond_wait(&lock_status[lid].incond, &mutex);

  if (lock_status[lid].statu == NONE)
    lock_status[lid].statu = ACQUIRING;

  else if (lock_status[lid].statu == FREE)
  {
    lock_status[lid].statu = LOCKED;
    pthread_mutex_unlock(&mutex);
    return ret;
  }

  while (true)
  {
    int r;
    pthread_mutex_unlock(&mutex);
    ret = cl->call(lock_protocol::acquire, lid, id, r);
    pthread_mutex_lock(&mutex);
    if (ret == lock_protocol::RETRY)
    {
      while(lock_status[lid].retry == false)
        pthread_cond_wait(&lock_status[lid].outcond, &mutex);
      lock_status[lid].retry = false;
    }
    else if (ret == lock_protocol::OK)
    {
      lock_status[lid].statu = LOCKED;
      break;
    }
  }
  
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  pthread_mutex_lock(&mutex);
  int ret = lock_protocol::OK;

  if (lock_status[lid].revoke == true)
  {
    lock_status[lid].statu = RELEASING;
    lock_status[lid].revoke = false;

    int r;
    pthread_mutex_unlock(&mutex);
    ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&mutex);

    lock_status[lid].statu = NONE;
    pthread_cond_signal(&lock_status[lid].incond);
  }
  else if (lock_status[lid].statu == LOCKED)
  {
    lock_status[lid].statu = FREE;
    pthread_cond_signal(&lock_status[lid].incond);
  }

  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  pthread_mutex_lock(&mutex);
  int ret = rlock_protocol::OK;

  lock_status[lid].revoke = true;

  if (lock_status[lid].statu == FREE)
  {
    lock_status[lid].statu = RELEASING;

    int r;
    pthread_mutex_unlock(&mutex);      
    ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&mutex);

    lock_status[lid].statu = NONE;
    lock_status[lid].revoke = false;
    pthread_cond_signal(&lock_status[lid].incond);
  }

  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  pthread_mutex_lock(&mutex);

  int ret = rlock_protocol::OK;
  pthread_cond_signal(&(lock_status[lid].outcond));
  lock_status[lid].retry = true;
  pthread_mutex_unlock(&mutex);
  return ret;
}
