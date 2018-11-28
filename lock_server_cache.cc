// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

lock_server_cache::lock_server_cache():
	nacquire (0)
{
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  pthread_mutex_lock(&mutex);
  lock_protocol::status ret = lock_protocol::OK;

  if (lock_status.find(lid) == lock_status.end())
  {
    lock_statu temp;
    temp.statu = LOCKED;
    temp.id = id;
    temp.retry = "";
    lock_status[lid] = temp;
    pthread_mutex_unlock(&mutex);
    return ret;
  }

  if (lock_status[lid].statu == UNLOCKED)
  {
    lock_status[lid].statu = LOCKED;
    lock_status[lid].id = id;
    pthread_mutex_unlock(&mutex);
    return ret;
  }

  else if (lock_status[lid].queue.empty())
  {
    lock_status[lid].statu = LOCKED;
    lock_status[lid].queue.insert(id);
    lock_status[lid].retry = id;
    ret = lock_protocol::RETRY;
  }

  else
  {
    if (lock_status[lid].queue.find(lock_status[lid].retry) == lock_status[lid].queue.end())
    {
      ret = lock_protocol::RETRY;
      lock_status[lid].queue.insert(id);
    }
    else
    {
      if (lock_status[lid].retry == id)
      {
        lock_status[lid].id = id;
  	lock_status[lid].queue.erase(lock_status[lid].queue.find(id));
  	if (lock_status[lid].queue.empty())
  	{
  	  lock_status[lid].retry = "";
	  pthread_mutex_unlock(&mutex);
  	  return ret;
  	}
  	else
  	  lock_status[lid].retry = *(lock_status[lid].queue.begin());
      }
      else
      {
  	ret = lock_protocol::RETRY;
  	lock_status[lid].queue.insert(id);
	pthread_mutex_unlock(&mutex);
  	return ret;
      }
    }
  }

  handle h(lock_status[lid].id);
  rpcc *cl = h.safebind();

  pthread_mutex_unlock(&mutex);
  int r = cl->call(rlock_protocol::revoke, lid, r);
  pthread_mutex_lock(&mutex);

  pthread_mutex_unlock(&mutex);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &r)
{
  pthread_mutex_lock(&mutex);
  lock_protocol::status ret = lock_protocol::OK;
  lock_status[lid].id = "";

  if (lock_status[lid].queue.empty())
  {
    lock_status[lid].statu = UNLOCKED;
    pthread_mutex_unlock(&mutex);
    return ret;
  }


  handle h(lock_status[lid].retry);
  rpcc *cl = h.safebind();

  pthread_mutex_unlock(&mutex);
  r = cl->call(rlock_protocol::retry, lid, r);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}
