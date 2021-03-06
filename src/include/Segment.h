#ifndef _HLKVDS_SEGMENT_H_
#define _HLKVDS_SEGMENT_H_

#include <string>
#include <sys/types.h>

#include <list>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include "KeyDigestHandle.h"
#include "Db_Structure.h"
#include "Utils.h"

namespace hlkvds {

class SegmentOnDisk;
class DataHeader;
class DataHeaderOffset;
class HashEntry;
class IndexManager;
class Volumes;
class SegForReq;

class KVSlice {
public:
    KVSlice();
    ~KVSlice();
    KVSlice(const KVSlice& toBeCopied);
    KVSlice& operator=(const KVSlice& toBeCopied);

    KVSlice(const char* key, int key_len, const char* data, int data_len, bool deep_copy = false);
    KVSlice(Kvdb_Digest *digest, const char* key, int key_len,
            const char* data, int data_len);

    const Kvdb_Digest& GetDigest() const {
        return *digest_;
    }

    const char* GetKey() const {
        return key_;
    }

    const char* GetData() const {
        return data_;
    }

    std::string GetKeyStr() const;
    std::string GetDataStr() const;

    uint16_t GetKeyLen() const {
        return keyLength_;
    }
    uint16_t GetDataLen() const {
        return dataLength_;
    }

    bool IsAlignedData() const{
        return GetDataLen() == ALIGNED_SIZE;
    }

    HashEntry& GetHashEntry() const {
        return *entry_;
    }

    uint32_t GetSegId() const {
        return segId_;
    }

    void SetKeyValue(const char* key, int key_len, const char* data, int data_len);
    void SetHashEntry(const HashEntry *hash_entry);
    void SetSegId(uint32_t seg_id);


private:
    const char* key_;
    uint32_t keyLength_;
    const char* data_;
    uint16_t dataLength_;
    Kvdb_Digest *digest_;
    HashEntry *entry_;
    uint32_t segId_;
    bool deepCopy_;

    void copy_helper(const KVSlice& toBeCopied);
    void calcDigest();

};

class Request {
public:
    enum ReqStat {
        INIT = 0,
        FAIL,
        SUCCESS
    };

public:
    Request();
    ~Request();
    Request(const Request& toBeCopied);
    Request& operator=(const Request& toBeCopied);
    Request(KVSlice& slice);

    KVSlice& GetSlice() const {
        return *slice_;
    }

    bool GetWriteStat() const {
        return stat_ == ReqStat::SUCCESS;
    }

    void SetWriteStat(bool stat);

    void SetSeg(SegForReq *seg) {
        segPtr_ = seg;
    }

    SegForReq* GetSeg() {
        return segPtr_;
    }

    void SetShardsWQId(int shards_id) {
        shardsWqId_ = shards_id;
    }

    int GetShardsWQId() {
        return shardsWqId_;
    }

    void Wait();
    void Signal();

private:
    bool done_;
    ReqStat stat_;
    KVSlice *slice_;
    mutable std::mutex mtx_;
    std::condition_variable cv_;

    SegForReq *segPtr_;
    int shardsWqId_;
};

class SegBase {
public:
    SegBase();
    ~SegBase();
    SegBase(const SegBase& toBeCopied);
    SegBase& operator=(const SegBase& toBeCopied);
    SegBase(Volumes* vol);

    bool TryPut(KVSlice* slice);
    void Put(KVSlice* slice);
    bool WriteSegToDevice();
    uint32_t GetFreeSize() const {
        return tailPos_ - headPos_;
    }

    int32_t GetSegId() const {
        return segId_;
    }
    void SetSegId(int32_t seg_id) {
        segId_ = seg_id;
    }

    int32_t GetKeyNum() const {
        return keyNum_;
    }

    std::list<KVSlice *>& GetSliceList() {
        return sliceList_;
    }

    Volumes* GetSelfVolume() {
        return vol_;
    }

private:
    void copyHelper(const SegBase& toBeCopied);
    void fillEntryToSlice();
    bool _writeDataToDevice();
    void copyToDataBuf();
    bool newDataBuffer();

private:
    int32_t segId_;
    Volumes* vol_;
    int32_t segSize_;
    KVTime persistTime_;

    uint32_t headPos_;
    uint32_t tailPos_;

    int32_t keyNum_;
    int32_t keyAlignedNum_;

    std::list<KVSlice *> sliceList_;

    SegmentOnDisk *segOndisk_;
    char *dataBuf_;
};

class SegForReq : public SegBase {
public:
    SegForReq();
    ~SegForReq();
    SegForReq(const SegForReq& toBeCopied);
    SegForReq& operator=(const SegForReq& toBeCopied);

    SegForReq(Volumes* vol, IndexManager* im, uint32_t timeout);

    bool TryPut(Request* req);
    void Put(Request* req);
    void Completion();
    void Notify(bool stat);
    bool IsExpired();

    int32_t CommitedAndGetNum() {
        return --reqCommited_;
    }

    void CleanDeletedEntry();

private:
    IndexManager* idxMgr_;
    uint32_t timeout_;
    KVTime startTime_;
    KVTime persistTime_;

    bool isCompletion_;
    bool hasReq_;

    std::atomic<int32_t> reqCommited_;
    std::list<Request *> reqList_;

    mutable std::mutex mtx_;
    std::list<HashEntry> delReqList_;
};

class SegForSlice : public SegBase {
public:
    SegForSlice();
    ~SegForSlice();
    SegForSlice(const SegForSlice& toBeCopied);
    SegForSlice& operator=(const SegForSlice& toBeCopied);

    SegForSlice(Volumes* vol, IndexManager* im);

    void UpdateToIndex();
private:
    IndexManager* idxMgr_;
};

}
#endif //#ifndef _HLKVDS_SEGMENT_H_
