// Copyright [2017] <Intel>
#include <hlkvds/Kvdb.h>
#include <hlkvds/Options.h>
#include "libckvdb.h"

void kvdb_aio_release(kvdb_completion_t c){
  //hdcs::AioCompletion *comp = (hdcs::C_AioRequestCompletion*) c;
  //delete comp;
}

void kvdb_aio_wait_for_complete(kvdb_completion_t c){
  //hdcs::AioCompletion *comp = (hdcs::C_AioRequestCompletion*) c;
  //comp->wait_for_complete();
}

int kvdb_aio_create_completion(void *cb_arg, callback_t complete_cb, kvdb_completion_t *c){
  //hdcs::AioCompletion *comp = new hdcs::C_AioRequestCompletion(cb_arg, complete_cb);
  //*c = (hdcs_completion_t) comp;
  return 0;
}

ssize_t kvdb_aio_get_return_value(kvdb_completion_t c) {
  //hdcs::AioCompletion *comp = (hdcs::C_AioRequestCompletion*) c;
  //return comp->get_return_value();
  return 0;
}

int kvdb_open(kvdb_ioctx_t *io) {
  hlkvds::Options opts;
  opts.hashtable_size = 1280000;
  opts.gc_upper_level = 0.7;
  opts.disable_cache = 1;
  
  
    hlkvds::DB::OpenDB("/dev/sdb2", (hlkvds::DB **)io, opts);
  return 0;
}

int kvdb_close(kvdb_ioctx_t io) {
  /*if(io){
    delete (hlkvds::DB *)io;
  }*/
  return 0;
}

int kvdb_aio_read(kvdb_ioctx_t io, char* data, uint64_t offset, uint64_t length, kvdb_completion_t c){
  //void* arg = (void*)c;
  //((hdcs::core::HDCSCore*)io)->aio_read(data, offset, length, arg);
  char buff[21];
  snprintf(buff, sizeof(buff), "%" PRIu64, offset);
  string get_data;
  ((hlkvds::DB *)io)->Get(buff, strlen(buff), get_data);
  return 0;
}

int kvdb_aio_write(kvdb_ioctx_t io, const char* data, uint64_t offset, uint64_t length, kvdb_completion_t c){
  //void* arg = (void*)c;
  //((hdcs::core::HDCSCore*)io)->aio_write(data, offset, length, arg);
  char buff[21];
  snprintf(buff, sizeof(buff), "%" PRIu64, offset);
  ((hlkvds::DB *)io)->Insert(buff, strlen(buff), data, length);
  return 0;
}
