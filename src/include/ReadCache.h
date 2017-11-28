#ifndef READ_CACHE_H
#define READ_CACHE_H
#include <iostream>
#include <string.h>
#include <map>
#include <mutex>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include "CacheMap.h"
#include "KeyDigestHandle.h"
#include "Db_Structure.h"
#include "hlkvds/Options.h"
#include "Utils.h"
namespace dslab{
//typedef boost::shared_mutex smutex;
//typedef boost::unique_lock< smutex > WriteLock;
//typedef boost::shared_lock< smutex > ReadLock;
//typedef std::lock_guard < std::mutex > smutex;

class ReadCache{
	public:
		ReadCache(CachePolicy policy, size_t cache_size = 1024, int percent = 50, bool is_dedup = 0);
		~ReadCache();
		void Put(std::string key, std::string value);
		bool Get(std::string key, std::string& value);
		void Delete(std::string key);
	private:
		CacheMap<std::string, std::string>* cache_map;//map<footprint,value>
        std::map<std::string, std::string> dedup_map;//map<key,footprint>
        std::multimap<std::string, std::string> refer_map;//<footprint,keys>
		hlkvds::KeyDigestHandle *em;//input digest to footprint
//		smutex myLock;
		std::mutex mtx_;
		bool isDedup;
};

}

#endif
