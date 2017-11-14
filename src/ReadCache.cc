#include "ReadCache.h"

using namespace std;

namespace dslab{
#ifdef FIO
        int hits = 0;
        int hitCandidates = 0;
#endif

ReadCache::ReadCache(CachePolicy policy, size_t cache_size, int percent){
	cache_map = CacheMap<string,string>::create(policy, cache_size, cache_size*(100-percent)/100);
	em = NULL;
}


ReadCache::~ReadCache(){
#ifdef FIO
        if(dslab::hitCandidates!=0){
                double hitRate = (double)hits/(double)hitCandidates;
		cout<<"hits: "<<hits<<"; Candidates: "<<hitCandidates<<endl;
                cout<<"hit rate: "<<hitRate<<endl;
        }
#endif
	delete cache_map;
}

void ReadCache::Put(string key, string value){
	//get footprint
//	WriteLock w_lock(myLock);
	std::lock_guard < std::mutex > l(mtx_);
	hlkvds::Kvdb_Key input(value.c_str(),value.length());
	hlkvds::Kvdb_Digest result;
	em->ComputeDigest(&input,result);
	string footprint = em->Tostring(&result);
	string tobeUpdate = "", lkey ="", lvalue = "";//useless key, useless value
	map<string, string>::iterator it_dedup = dedup_map.find(key);//old_footprint
	multimap<string, string>::iterator it_refer;
	if( it_dedup!=dedup_map.end() ){//key already exist
		if(it_dedup->second != footprint){//renew the value
			it_refer = refer_map.find(it_dedup->second);
			for(int i = 0; i < refer_map.count(it_dedup->second); i++){
				if(it_refer->second == key) {
					refer_map.erase(it_refer);
					break;
				}
				it_refer++;
			}
			it_dedup->second = footprint;
			refer_map.insert(pair<string, string>(it_dedup->second, key));
			cache_map->Put(it_dedup->second, value, lkey, lvalue);
		}else{ //same value same key
			cache_map->Put(it_dedup->second, value, lkey, lvalue, true);
		}
	}else{//key does not exist
		dedup_map.insert(pair<string,string>(key,footprint));
		refer_map.insert(pair<string,string>(footprint, key));
		if( cache_map->Put(footprint,value, tobeUpdate, lvalue) ){//cache has deleted an entry
			it_refer = refer_map.find(tobeUpdate);
			while(it_refer!=refer_map.end()){
				it_dedup = dedup_map.find(it_refer->second);
				dedup_map.erase(it_dedup);
				refer_map.erase(it_refer);

				it_refer= refer_map.find(tobeUpdate);
			}
		}
	}	
}

bool ReadCache::Get(string key, string &value){
//	ReadLock r_lock(myLock);
	std::lock_guard < std::mutex > l(mtx_);
	map<string, string>::iterator it_dedup = dedup_map.find(key);
	#ifdef FIO
	hitCandidates++;
	#endif
	if( it_dedup!=dedup_map.end() ){
	#ifdef FIO
		hits++;
	#endif
		return cache_map->Get(it_dedup->second, value);
	}
	else{ 
		value = "";
		return false;
	}
}

void ReadCache::Delete(string key){
	//WriteLock w_lock(myLock);
	std::lock_guard < std::mutex > l(mtx_);
	map<string, string>::iterator it_dedup = dedup_map.find(key);
	if(it_dedup!=dedup_map.end()){
		multimap<string, string>::iterator it_refer = refer_map.find( it_dedup->second);	
		for(int i = 0; i < refer_map.count( it_dedup->second); i++){
			if(it_refer->second == key) {
				refer_map.erase(it_refer);
				break;
			}
		}
		if(refer_map.count(it_dedup->second)==0) cache_map->Delete(it_dedup->second);
		dedup_map.erase(it_dedup);
	}
}
}
