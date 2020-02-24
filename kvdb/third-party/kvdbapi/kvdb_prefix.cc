#include "kvdb_test.h"
#include "perf_context.h"

using namespace std;

namespace rocksdb {

struct TestKey {
    uint64_t prefix;
    uint64_t sorted;

    TestKey(uint64_t _prefix, uint64_t _sorted)
            : prefix(_prefix), sorted(_sorted) {}
};

// return a slice backed by test_key
inline Slice TestKeyToSlice(std::string &s, const TestKey& test_key) {
    s.clear();
    PutFixed64(&s, test_key.prefix);
    PutFixed64(&s, test_key.sorted);
    return Slice(s.c_str(), s.size());
}

inline const TestKey SliceToTestKey(const Slice& slice) {
    return TestKey(DecodeFixed64(slice.data()),
        DecodeFixed64(slice.data() + 8));
}

class TestKeyComparator : public Comparator {
 public:

    // Compare needs to be aware of the possibility of a and/or b is
    // prefix only
    virtual int Compare(const Slice& a, const Slice& b) const override {
        const TestKey kkey_a = SliceToTestKey(a);
        const TestKey kkey_b = SliceToTestKey(b);
        const TestKey *key_a = &kkey_a;
        const TestKey *key_b = &kkey_b;
        if (key_a->prefix != key_b->prefix) {
            if (key_a->prefix < key_b->prefix) return -1;
            if (key_a->prefix > key_b->prefix) return 1;
        } else {
            EXPECT_TRUE(key_a->prefix == key_b->prefix);
            // note, both a and b could be prefix only
            if (a.size() != b.size()) {
                // one of them is prefix
                EXPECT_TRUE(
                        (a.size() == sizeof(uint64_t) && b.size() == sizeof(TestKey)) ||
                        (b.size() == sizeof(uint64_t) && a.size() == sizeof(TestKey)));
                if (a.size() < b.size()) return -1;
                if (a.size() > b.size()) return 1;
            } else {
                // both a and b are prefix
                if (a.size() == sizeof(uint64_t)) {
                    return 0;
                }

                // both a and b are whole key
                EXPECT_TRUE(a.size() == sizeof(TestKey) && b.size() == sizeof(TestKey));
                if (key_a->sorted < key_b->sorted) return -1;
                if (key_a->sorted > key_b->sorted) return 1;
                if (key_a->sorted == key_b->sorted) return 0;
            }
        }
        return 0;
    }

    bool operator()(const TestKey& a, const TestKey& b) const {
        std::string sa, sb;
        return Compare(TestKeyToSlice(sa, a), TestKeyToSlice(sb, b)) < 0;
    }

    virtual const char* Name() const override {
        return "TestKeyComparator";
    }

    virtual void FindShortestSeparator(std::string* start,
                                                                         const Slice& limit) const override {}

    virtual void FindShortSuccessor(std::string* key) const override {}
};

namespace {
void PutKey(DB* db_, WriteOptions wropt, uint64_t prefix,
                        uint64_t suffix, const Slice& value) {
    TestKey test_key(prefix, suffix);
    std::string s;
    Slice key = TestKeyToSlice(s, test_key);
    db_->Put(wropt, key, value);
}
void PutKey(DB* db_, WriteOptions wropt, const TestKey& test_key,
                        const Slice& value) {
    std::string s;
    Slice key = TestKeyToSlice(s, test_key);
    db_->Put(wropt, key, value);
}

void MergeKey(DB* db_, WriteOptions wropt, const TestKey& test_key,
                            const Slice& value) {
    std::string s;
    Slice key = TestKeyToSlice(s, test_key);
    db_->Merge(wropt, key, value);
}

void DeleteKey(DB* db_, WriteOptions wropt, const TestKey& test_key) {
    std::string s;
    Slice key = TestKeyToSlice(s, test_key);
    db_->Delete(wropt, key);
}

void SeekIterator(Iterator* iter, uint64_t prefix, uint64_t suffix) {
    TestKey test_key(prefix, suffix);
    std::string s;
    Slice key = TestKeyToSlice(s, test_key);
    iter->Seek(key);
}

const std::string kNotFoundResult = "NOT_FOUND";

std::string GetKey(DB* db_, const ReadOptions& ropt, uint64_t prefix,
                                uint64_t suffix) {
    TestKey test_key(prefix, suffix);
    std::string s2;
    Slice key = TestKeyToSlice(s2, test_key);

    std::string result;
    Status s = db_->Get(ropt, key, &result);
    if (s.IsNotFound()) {
        result = kNotFoundResult;
    } else if (!s.ok()) {
        result = s.ToString();
    }
    return result;
}

class SamePrefixTransform : public SliceTransform {
 private:
    const Slice prefix_;
    std::string name_;

 public:
    explicit SamePrefixTransform(const Slice& prefix)
            : prefix_(prefix), name_("rocksdb.SamePrefix." + prefix.ToString()) {}

    virtual const char* Name() const override { return name_.c_str(); }

    virtual Slice Transform(const Slice& src) const override {
        assert(InDomain(src));
        return prefix_;
    }

    virtual bool InDomain(const Slice& src) const override {
        if (src.size() >= prefix_.size()) {
            return Slice(src.data(), prefix_.size()) == prefix_;
        }
        return false;
    }

    virtual bool InRange(const Slice& dst) const override {
        return dst == prefix_;
    }
};
}    // namespace


class PrefixTest : public KVDBTest {
 public:
        PrefixTest() : option_config_(kBegin) {
        dbname_ = test::TmpDir() +"/Prefix_test";
        options.create_if_missing = true;
        options.write_buffer_size = 33554432;
        options.max_write_buffer_number = 2;
        options.min_write_buffer_number_to_merge =1;
        //options.memtable_prefix_bloom_size_ratio =01;
        //options.memtable_huge_page_size = 2 * 1024 * 1024;
        DestroyDB(dbname_, options);
        DB::Open(options, dbname_,&db_);
    }

    void FirstOption() {
        option_config_ = kBegin;
    }

    bool NextOptions(int bucket_count) {
        // skip some options
        option_config_++;
        if (option_config_ < kEnd) {
            options.prefix_extractor.reset(NewFixedPrefixTransform(8));
            switch(option_config_) {
                case kHashSkipList:
                    options.memtable_factory.reset(
                            NewHashSkipListRepFactory(bucket_count, 4));
                    return true;
                case kHashLinkList:
                    options.memtable_factory.reset(
                            NewHashLinkListRepFactory(bucket_count));
                    return true;
                case kHashLinkListHugePageTlb:
                    options.memtable_factory.reset(
                            NewHashLinkListRepFactory(bucket_count, 2 * 1024 * 1024));
                    return true;
                case kHashLinkListTriggerSkipList:
                    options.memtable_factory.reset(
                            NewHashLinkListRepFactory(bucket_count, 0, 3));
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }


    ~PrefixTest() {
        Destroy(options);
    }
    int option_config_;
    Options options;
    WriteOptions wropt;
    ReadOptions ropt;
 protected:
    enum OptionConfig {
        kBegin,
        kHashSkipList,
        kHashLinkList,
        kHashLinkListHugePageTlb,
        kHashLinkListTriggerSkipList,
        kEnd
    };
};
/*************************************************************************************************/
/**********************************************Prefix Test Case***********************************/
/*************************************************************************************************/
/**
 * @brief      Use Prefix_Zero test Prefix same as start prefix is 0
 * @param      PrefixTest --- Class name. \n
 *             Prefix_Zero --- Function name.
 * @return     void
 * @author     Liu Liu(ll66.liu@sumsung.com)
 * @version    v0.1
 * @remark
 */
TEST_F(PrefixTest, Prefix_Zero){
    options.prefix_extractor.reset(NewFixedPrefixTransform(0));
    ropt.prefix_same_as_start = true;
    ropt.iterate_upper_bound = nullptr;

    DestroyAndReopen(options);
    ASSERT_OK(db_->Put(wropt, "1", "1"));
    ASSERT_OK(db_->Put(wropt, "23", "2"));
    ASSERT_OK(db_->Put(wropt, "445", "1"));
    ASSERT_OK(db_->Put(wropt, "56", "1"));

    auto iter = db_->NewIterator(ropt);
    cout<<"start to seek"<<endl;
    iter -> Seek("3");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("445",iter->key().ToString());
   
    iter->Next();
	ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("56",iter->key().ToString());
	
	iter->Next();
    ASSERT_TRUE(!iter->Valid());
    
    delete iter;
}

TEST_F(PrefixTest, Prefix_Special){
    options.prefix_extractor.reset(NewFixedPrefixTransform(2));
    ropt.prefix_same_as_start = true;
    ropt.iterate_upper_bound = nullptr;

    DestroyAndReopen(options);
	
    ASSERT_OK(db_->Put(wropt, "!@", "1"));
    ASSERT_OK(db_->Put(wropt, "#$", "2"));
    ASSERT_OK(db_->Put(wropt, "~%^", "1"));
    ASSERT_OK(db_->Put(wropt, "& *", "1"));

    auto iter = db_->NewIterator(ropt);
    cout<<"start to seek"<<endl;
    iter -> Seek("!@");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("!@",iter->key().ToString());
	
	iter->Next();
    ASSERT_TRUE(!iter->Valid());
    
    delete iter;
}
/**
 * @brief      Use Prefix_OneNext test Prefix same as start and seek Next function
 * @param      PrefixTest --- Class name. \n
 *             Prefix_OneNext --- Function name.
 * @return     void
 * @author     Liu Liu(ll66.liu@sumsung.com)
 * @version    v0.1
 * @remark
 */
TEST_F(PrefixTest, Prefix_OneNext){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.prefix_same_as_start = true;
    ropt.iterate_upper_bound = nullptr;
    
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "1", "1"));
    ASSERT_OK(db_->Put(wropt, "23", "2"));
    ASSERT_OK(db_->Put(wropt, "145", "1"));
    ASSERT_OK(db_->Put(wropt, "a1", "1"));
    ASSERT_OK(db_->Put(wropt, "b1", "3"));
    ASSERT_OK(db_->Put(wropt, "167", "3"));
    ASSERT_OK(db_->Put(wropt, "a", "3"));
    cout<<"Finish to put data in to DB"<<endl;
    
    cout<<"create a new iterator"<<endl;
    auto iter = db_->NewIterator(ropt);
    cout<<"start to seek"<<endl;
    iter -> Seek("1");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("1",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("145",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("167",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    
    delete iter;
    }

/**
 * @brief      Use Prefix_OnePrev test Prefix same as start and seek Prev function
 * @param      PrefixTest --- Class name. \n
 *             Prefix_OnePrev --- Function name.
 * @return     void
 * @author     Liu Liu(ll66.liu@sumsung.com)
 * @version    v0.1
 * @remark
 */
TEST_F(PrefixTest, Prefix_OnePrev){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.prefix_same_as_start = true;
    ropt.iterate_upper_bound = nullptr;
    
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "a", "1"));
    ASSERT_OK(db_->Put(wropt, "test", "1"));
    ASSERT_OK(db_->Put(wropt, "a2", "3"));
    ASSERT_OK(db_->Put(wropt, "b", "3"));
    cout<<"Finish to put data in to DB"<<endl;

    cout<<"create a new iterator"<<endl;
    auto iter = db_->NewIterator(ropt);
    cout<<"start to seek"<<endl;
    
    iter -> Seek("test");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("test",iter->key().ToString());
    
    iter->Prev();
    ASSERT_TRUE(!iter->Valid());

    delete iter;
    }
    

/**
 * @brief      Use Prefix_InvaildKey test seek invaild key function
 * @param      PrefixTest --- Class name. \n
 *             Prefix_InvaildKey --- Function name.
 * @return     void
 * @author     Liu Liu(ll66.liu@sumsung.com)
 * @version    v0.1
 * @remark
 */
TEST_F(PrefixTest, Prefix_InvaildKey){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.prefix_same_as_start = true;
    ropt.iterate_upper_bound = nullptr;
    
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "a", "1"));
    ASSERT_OK(db_->Put(wropt, "test", "1"));
    ASSERT_OK(db_->Put(wropt, "a2", "3"));
    ASSERT_OK(db_->Put(wropt, "b", "3"));
    cout<<"Finish to put data in to DB"<<endl;

    cout<<"create a new iterator"<<endl;
    auto iter = db_->NewIterator(ropt);
    cout<<"start to seek"<<endl;
    
    iter -> Seek("e");
    ASSERT_TRUE(!iter->Valid());

    delete iter;
    }
/**
 * @brief         Use Prefix_SameAsLast test Prefix same as start and seekToLast function
 * @param         PrefixTest --- Class name. \n
 *                Prefix_FromLast --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_FromLast){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.prefix_same_as_start = true;
    
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "a", "1"));
    ASSERT_OK(db_->Put(wropt, "test", "1"));
    ASSERT_OK(db_->Put(wropt, "a2", "3"));
    ASSERT_OK(db_->Put(wropt, "b", "3"));
    cout<<"Finish to put data in to DB"<<endl;

    cout<<"create a new iterator"<<endl;
    auto iter = db_->NewIterator(ropt);
    //prefix_start_key is null,so not match
    iter->SeekToLast();
    ASSERT_TRUE(!iter->Valid());
    
    iter -> Seek("foo");
    ASSERT_TRUE(!iter->Valid());
    delete iter;
    }
/**
 * @brief         Use Prefix_SameAsFrist test Prefix same as start and SeekToFirst function
 * @param         PrefixTest --- Class name. \n
 *                Prefix_FromFrist --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_FromFrist){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.prefix_same_as_start = true;
    
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "t", "1"));
    ASSERT_OK(db_->Put(wropt, "test", "1"));
    ASSERT_OK(db_->Put(wropt, "a2", "3"));
    ASSERT_OK(db_->Put(wropt, "b", "3"));
    ASSERT_OK(db_->Put(wropt, "a3", "3"));
    cout<<"Finish to put data in to DB"<<endl;

    cout<<"create a new iterator"<<endl;
    auto iter = db_->NewIterator(ropt);
    //prefix_start_key is null,so not match
    iter-> SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a2",iter->key().ToString());
    
    iter-> Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a3",iter->key().ToString());
    delete iter;
    }
    
/**
 * @brief         Use Prefix_FromFristandSeek test Prefix same as start and SeekToFirst function
 * @param         PrefixTest --- Class name. \n
 *                Prefix_FromFristandSeek --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_FromFristandSeek){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.prefix_same_as_start = true;
    
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "t", "1"));
    ASSERT_OK(db_->Put(wropt, "test", "1"));
    ASSERT_OK(db_->Put(wropt, "a2", "3"));
    ASSERT_OK(db_->Put(wropt, "b", "3"));
    ASSERT_OK(db_->Put(wropt, "a3", "3"));
    cout<<"Finish to put data in to DB"<<endl;

    cout<<"create a new iterator"<<endl;
    auto iter = db_->NewIterator(ropt);
    iter -> SeekToFirst();
    iter -> Seek("t2");
    //test>t2>t,so t can't seek 
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("test",iter->key().ToString());
    iter-> Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
    }
/**
 * @brief         Use Prefix_FromFristandSeek test Prefix same as start and SeekToFirst function
 * @param         PrefixTest --- Class name. \n
 *                Prefix_NoSameStartWithSeekForPrev --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_NoSameStartWithSeekForPrev){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.prefix_same_as_start = false;
    ropt.iterate_upper_bound = nullptr;
    
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "b", "1"));
    ASSERT_OK(db_->Put(wropt, "b1", "1"));
    ASSERT_OK(db_->Put(wropt, "d", "3"));
    ASSERT_OK(db_->Put(wropt, "d4", "3"));
    ASSERT_OK(db_->Put(wropt, "a1", "3"));
    cout<<"Finish to put data in to DB"<<endl;

    cout<<"create a new iterator"<<endl;
    auto iter = db_->NewIterator(ropt);
    iter-> SeekForPrev("d3");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("d",iter->key().ToString());
    
    iter-> Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("d4",iter->key().ToString());
    
    iter-> Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("d",iter->key().ToString());
    
    iter-> Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("b1",iter->key().ToString());
    
    iter-> Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("b",iter->key().ToString());
    
    iter-> Prev();
    ASSERT_TRUE(iter->Valid());
    cout<<iter->key().ToString()<<endl;
    ASSERT_EQ("a1",iter->key().ToString());
    
    iter-> Prev();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
 }
/**
 * @brief         Use Prefix_NoSameStart test Prefix is not same as start function
 * @param         PrefixTest --- Class name. \n
 *                Prefix_NoSameStart --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_NoSameStart){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    //set presfix not same as start
    ropt.prefix_same_as_start = false;
    ropt.iterate_upper_bound = nullptr;
    
    
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "a", "1"));
    ASSERT_OK(db_->Put(wropt, "a1", "1"));
    ASSERT_OK(db_->Put(wropt, "foo", "1"));
    ASSERT_OK(db_->Put(wropt, "foo1", "3"));
    ASSERT_OK(db_->Put(wropt, "a2", "3"));
    ASSERT_OK(db_->Put(wropt, "b2", "3"));

    auto iter = db_->NewIterator(ropt);
    cout<<"start to seek"<<endl;
    iter -> Seek("c");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo1",iter->key().ToString());
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
    }
/**
 * @brief         Use Prefix_SameStart to test Prefix seek based on same prefix
 * @param         PrefixTest --- Class name. \n
 *                Prefix_SameStart --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_SameStart){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    //set presfix not same as start
    ropt.prefix_same_as_start = true;
    ropt.iterate_upper_bound = nullptr;
    
    
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "a", "1"));
    ASSERT_OK(db_->Put(wropt, "a1", "1"));
    ASSERT_OK(db_->Put(wropt, "foo", "1"));
    ASSERT_OK(db_->Put(wropt, "foo1", "3"));
    ASSERT_OK(db_->Put(wropt, "a2", "3"));
    ASSERT_OK(db_->Put(wropt, "b2", "3"));
    auto iter = db_->NewIterator(ropt);
    cout<<"start to seek"<<endl;
    iter -> Seek("c");
    ASSERT_TRUE(!iter->Valid());
   
    delete iter;
    }
/**
 * @brief         Use Prefix_SameMultiTransform test Prefix Transform is 3 function
 * @param         PrefixTest --- Class name. \n
 *                Prefix_SameMultiTransform --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_SameMultiTransform){
    options.prefix_extractor.reset(NewFixedPrefixTransform(3));
    ropt.iterate_upper_bound = nullptr;
    ropt.prefix_same_as_start = true;
 
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "key1", "value1"));
    ASSERT_OK(db_->Put(wropt, "key2", "value2"));
    ASSERT_OK(db_->Put(wropt, "others", "value5"));
    ASSERT_OK(db_->Put(wropt, "abc", "66"));
    ASSERT_OK(db_->Put(wropt, "cde", "77"));
    cout<<"Finish to put data in to DB"<<endl;
    cout<<"create a new iterator"<<endl;
    auto iter = db_->NewIterator(ropt);
    
    cout<<"start to seek"<<endl;
    iter ->Seek("value");
    ASSERT_TRUE(!iter->Valid());
    delete iter;
    
    iter = db_->NewIterator(ropt);
    iter ->Seek("key0");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("key1",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("key2",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
    }
    /**
 * @brief         Use Prefix_MultiTransform test Prefix Transform is 3 function
 * @param         PrefixTest --- Class name. \n
 *                Prefix_MultiTransform --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_MultiTransform){
    options.prefix_extractor.reset(NewFixedPrefixTransform(3));
    ropt.iterate_upper_bound = nullptr;

    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "key1", "value1"));
    ASSERT_OK(db_->Put(wropt, "key2", "value2"));
    ASSERT_OK(db_->Put(wropt, "key4", "value4"));
    ASSERT_OK(db_->Put(wropt, "others", "value5"));
    ASSERT_OK(db_->Put(wropt, "abc", "66"));
    ASSERT_OK(db_->Put(wropt, "cde", "77"));
    cout<<"Finish to put data in to DB"<<endl;
    cout<<"create a new iterator"<<endl;
    auto iter = db_->NewIterator(ropt);
    
    cout<<"start to seek"<<endl;
    iter ->Seek("key0");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("key1",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("key2",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("key4",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("others",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
    }
    
/**
 * @brief         Use Prefix_MultiTransform2 test Prefix Transform is 3 function
 * @param         PrefixTest --- Class name. \n
 *                Prefix_MultiTransform2 --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_MultiTransform2){
    options.prefix_extractor.reset(NewFixedPrefixTransform(3));
    ropt.iterate_upper_bound = nullptr;
 
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "key1", "value1"));
    ASSERT_OK(db_->Put(wropt, "key2", "value2"));
    ASSERT_OK(db_->Put(wropt, "others", "value5"));
    ASSERT_OK(db_->Put(wropt, "abc", "66"));
    ASSERT_OK(db_->Put(wropt, "123", "77"));
    cout<<"Finish to put data in to DB"<<endl;
    cout<<"create a new iterator"<<endl;
    auto iter = db_->NewIterator(ropt);
    
    cout<<"start to seek"<<endl;
    //Seek Key is uppercase,but small than lowercase
    iter ->Seek("Key");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("abc",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("key1",iter->key().ToString());
    delete iter;
    
    }
/**
 * @brief         Use Prefix_MultiPrev test Prefix Prev
 * @param         PrefixTest --- Class name. \n
 *                Prefix_MultiPrev --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_MultiPrev){
    options.prefix_extractor.reset(NewFixedPrefixTransform(6));
    ropt.iterate_upper_bound = nullptr;
 
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "barbarbar", "foo"));
    ASSERT_OK(db_->Put(wropt, "barbarbar2", "foo2"));
    ASSERT_OK(db_->Put(wropt, "foo", "bar"));
    ASSERT_OK(db_->Put(wropt, "foo3", "bar3"));
    cout<<"Finish to put data in to DB"<<endl;
    cout<<"create a new iterator"<<endl;
    auto iter = db_->NewIterator(ropt);
    cout<<"start to seek"<<endl;
    
    iter ->SeekForPrev("foo2");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo",iter->key().ToString());
    cout<<iter->key().ToString()<<endl;

    iter -> Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("barbarbar2",iter->key().ToString());
    cout<<iter->key().ToString()<<endl;

    iter -> Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("barbarbar",iter->key().ToString());
    cout<<iter->key().ToString()<<endl;

    iter -> Prev();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
}
/**
 * @brief         Use Prefix_MultiTimes to  test Prefix seek multi times
 * @param         PrefixTest --- Class name. \n
 *                Prefix_MultiTimes --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version     v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_MultiTimes){
    options.prefix_extractor.reset(NewFixedPrefixTransform(6));
    ropt.iterate_upper_bound = nullptr;
 
    DestroyAndReopen(options);
    ASSERT_OK(db_->Put(wropt, "barbarbar", "foo"));
    ASSERT_OK(db_->Put(wropt, "barbarbar2", "foo2"));
    ASSERT_OK(db_->Put(wropt, "foo", "bar"));
    ASSERT_OK(db_->Put(wropt, "foo3", "bar3"));
    cout<<"create a new iterator"<<endl;
    for (int i=1;i<=1000;i++){
        auto iter = db_->NewIterator(ropt);
    
        iter ->SeekForPrev("foo2");
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("foo",iter->key().ToString());

        iter -> Prev();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("barbarbar2",iter->key().ToString());

        iter -> Prev();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("barbarbar",iter->key().ToString());
        
        iter -> Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("barbarbar2",iter->key().ToString());

        iter -> Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("foo",iter->key().ToString());

        iter -> Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("foo3",iter->key().ToString());

        iter -> Next();
        ASSERT_TRUE(!iter->Valid());
        delete iter;
    }
}
/**
 * @brief         Use Prefix_MultiData test Prefix seek kv in large data
 * @param         PrefixTest --- Class name. \n
 *                Prefix_MultiData --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_MultiData){
    options.prefix_extractor.reset(NewFixedPrefixTransform(3));
    ropt.iterate_upper_bound = nullptr;
    int Num = 10000;
    DestroyAndReopen(options);
    for (int i=1;i<=Num;i++){
        ASSERT_OK(db_->Put(wropt, "t"+ToString(i), "foo"+ToString(i)));
        cout<<"t"+ToString(i)<<endl;;
    }
        auto iter = db_->NewIterator(ropt);
    
        iter ->Seek("t505");
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("t505",iter->key().ToString());

        
        iter -> Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("t5050",iter->key().ToString());

        iter -> Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("t5051",iter->key().ToString());
        
        iter -> SeekToFirst();
        cout<<iter->key().ToString()<<endl;
        ASSERT_EQ("t1",iter->key().ToString());
        iter -> Next();
        cout<<iter->key().ToString()<<endl;
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("t10",iter->key().ToString());
        iter -> Next();
        cout<<iter->key().ToString()<<endl;
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("t100",iter->key().ToString());

        iter -> SeekToLast();
        cout<<iter->key().ToString()<<endl;
        ASSERT_EQ("t9999",iter->key().ToString());
        iter -> Prev();
        cout<<iter->key().ToString()<<endl;
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("t9998",iter->key().ToString());
        iter -> Prev();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("t9997",iter->key().ToString());
        delete iter;
		sleep(1);
    
}
/**
 * @brief         Use Prefix_SeekInCF test Prefix same as start and seek in Different CF function
 * @param         PrefixTest --- Class name. \n
 *                Prefix_SeekInCF --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_SeekInCF){
    std::vector<std::string> cf_names;
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(2));
    
    cf_names.push_back("one");
    CreateColumnFamilies(cf_names,options);
    cf_names.push_back("default");
    Close();
    ASSERT_OK(TryOpen(cf_names));
    ASSERT_OK(db_->Put(wropt, handles_[0], "abc", "11"));
    ASSERT_OK(db_->Put(wropt, handles_[0], "ac2", "22"));
    ASSERT_OK(db_->Put(wropt, handles_[0], "bc3", "33"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "b1", "44"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "b4", "55"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "bb", "77"));
    cout<<"start to seek"<<endl;
    auto iter = db_->NewIterator(ropt, handles_[0]);
    
    iter -> Seek("bc");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bc3",iter->key().ToString());
    cout<<"iter seek next"<<endl;
    iter -> Next();
    ASSERT_TRUE(!iter->Valid());

    delete iter;
    auto iter1 = db_->NewIterator(ropt, handles_[1]);
    
    iter1 -> Seek("b3");
    ASSERT_TRUE(iter1->Valid());
    ASSERT_EQ("b4",iter1->key().ToString());
        cout<<"iter1 seek next"<<endl;
    iter1 -> Next();
    ASSERT_TRUE(iter1->Valid());
    ASSERT_EQ("bb",iter1->key().ToString());
    iter1 -> Next();
    ASSERT_TRUE(!iter1->Valid());
    delete iter1;
    }

/**
 * @brief         Use Prefix_SeekPrevandNextinCF to test Prefix seek prev and next in Different CF
 * @param         PrefixTest --- Class name. \n
 *                Prefix_SeekPrevandNextinCF --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_SeekPrevandNextinCF){
    //DestroyAndReopen();
    std::vector<std::string> cf_names;
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(2));
    //DestroyAndReopen(options);
	
    cf_names.push_back("one");
    CreateColumnFamilies(cf_names,options);
    cf_names.push_back("default");
    Close();
    ASSERT_OK(TryOpen(cf_names));
    ASSERT_OK(db_->Put(wropt, handles_[0], "abc", "11"));
    ASSERT_OK(db_->Put(wropt, handles_[0], "ac2", "22"));
    ASSERT_OK(db_->Put(wropt, handles_[0], "bc3", "33"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "b1", "44"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "b4", "55"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "bb", "77"));

    cout<<"start to seek"<<endl;
    auto iter = db_->NewIterator(ropt, handles_[0]);
    iter -> Seek("bc");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bc3",iter->key().ToString());
    //Seek by prev
    cout<<"iter seek by prev"<<endl;
    iter -> Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("ac2",iter->key().ToString());
    iter -> Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("abc",iter->key().ToString());
    //seek by next
    cout<<"iter seek by next"<<endl;
    iter -> Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("ac2",iter->key().ToString());
    iter -> Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bc3",iter->key().ToString());
    iter -> Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
    //CF 1's iter and seek by prev and next
    auto iter1 = db_->NewIterator(ropt, handles_[1]);
    cout<<"iter1 seek by prev"<<endl;
    iter1 -> Seek("b3");
    ASSERT_TRUE(iter1->Valid());
    ASSERT_EQ("b4",iter1->key().ToString());

    iter1 -> Prev();
    ASSERT_TRUE(iter1->Valid());
    ASSERT_EQ("b1",iter1->key().ToString());
    cout<<"iter1 seek by next"<<endl;
    iter1 -> Next();
    ASSERT_TRUE(iter1->Valid());
    ASSERT_EQ("b4",iter1->key().ToString());
    iter1 -> Next();
    ASSERT_TRUE(iter1->Valid());
    ASSERT_EQ("bb",iter1->key().ToString());
    iter1 -> Next();
    ASSERT_TRUE(!iter1->Valid());
    delete iter1;
    }

/**
 * @brief         Use Prefix_TwoSeekinSameCF test Prefix seek by two iterators in one CF
 * @param         PrefixTest --- Class name. \n
 *                Prefix_TwoSeekinSameCF --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_TwoSeekinSameCF){
    //DestroyAndReopen();
    std::vector<std::string> cf_names;
    options.create_if_missing = true;
    options.prefix_extractor.reset(NewFixedPrefixTransform(2));
	//DestroyAndReopen(options);
	
    ropt.prefix_same_as_start = true;
    cf_names.push_back("one");
    CreateColumnFamilies(cf_names,options);
    cf_names.push_back("default");
    Close();
    ASSERT_OK(TryOpen(cf_names));
    ASSERT_OK(db_->Put(wropt, handles_[1], "abc", "11"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "ac2", "22"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "bc3", "33"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "foo", "44"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "Foo", "55"));
    ASSERT_OK(db_->Put(wropt, handles_[1], "Bc4", "66"));
    auto iter = db_->NewIterator(ropt, handles_[1]);
    auto iter1 = db_->NewIterator(ropt, handles_[1]);
    iter -> Seek("ac");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("ac2",iter->key().ToString());

    cout<<"iter seek"<<endl;
    iter -> Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bc3",iter->key().ToString());
    
    iter1 -> Seek("fo");
    ASSERT_TRUE(iter1->Valid());
    ASSERT_EQ("foo",iter1->key().ToString());

    iter -> Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo",iter->key().ToString());
    
    iter1 -> Next();
    ASSERT_TRUE(!iter1->Valid());

    iter -> Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter,iter1;
    }
/**
 * @brief         Use Prefix_SeekBeforePut test Prefix seek before put kv in the db
 * @param         PrefixTest --- Class name. \n
 *                Prefix_SeekBeforePut --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_SeekBeforePut){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.iterate_upper_bound = nullptr;
    ropt.prefix_same_as_start = true;

    ASSERT_OK(db_->Put(wropt, "ab", "value1"));
    ASSERT_OK(db_->Put(wropt, "cd", "value2"));
    ASSERT_OK(db_->Put(wropt, "fd", "value3"));
    ASSERT_OK(db_->Put(wropt, "123","value4"));
    ASSERT_OK(db_->Put(wropt, "AB", "value5"));

    auto iter = db_->NewIterator(ropt);
    //After create iterator then put kv in the db
    ASSERT_OK(db_->Put(wropt, "fg", "value6"));

    iter ->Seek("f00");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("fd",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
}
/**
 * @brief         Use Prefix_SeekBeforeDelete test Prefix seek before the kv is deleted
 * @param         PrefixTest --- Class name. \n
 *                Prefix_SeekBeforeDelete --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_SeekBeforeDelete){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.iterate_upper_bound = nullptr;
 
    DestroyAndReopen(options);
    ASSERT_OK(db_->Put(wropt, "ab", "value1"));
    ASSERT_OK(db_->Put(wropt, "cd", "value2"));
    ASSERT_OK(db_->Put(wropt, "fd", "value3"));
    ASSERT_OK(db_->Put(wropt, "123","value4"));
    ASSERT_OK(db_->Put(wropt, "fg", "value5"));

    auto iter = db_->NewIterator(ropt);

    ASSERT_OK(Delete("fg"));
    ASSERT_EQ("NOT_FOUND", Get("fg"));

    iter ->Seek("f00");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("fd",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("fg",iter->key().ToString());
    delete iter;

}
/**
 * @brief         Use Prefix_SeekAfterDelete test Prefix seek after the kv is deleted
 * @param         PrefixTest --- Class name. \n
 *                Prefix_SeekAfetrDelete --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_SeekAfterDelete){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.iterate_upper_bound = nullptr;
 
    DestroyAndReopen(options);
    ASSERT_OK(db_->Put(wropt, "ab", "value1"));
    ASSERT_OK(db_->Put(wropt, "cd", "value2"));
    ASSERT_OK(db_->Put(wropt, "fd", "value3"));
    ASSERT_OK(db_->Put(wropt, "123","value4"));
    ASSERT_OK(db_->Put(wropt, "AB", "value5"));

    auto iter = db_->NewIterator(ropt);
    
    iter ->Seek("f00");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("fd",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
    
    //Delete the key then seek
    ASSERT_OK(Delete("fd"));
    ASSERT_EQ("NOT_FOUND", Get("fd"));
    auto iter1 = db_->NewIterator(ropt);
    
    iter1 ->Seek("f00");
    ASSERT_TRUE(!iter1->Valid());
    delete iter1;
}
/**
 * @brief         Use Prefix_SeekAfterFlush test Prefix seek after the KVs are flushed
 * @param         PrefixTest --- Class name. \n
 *                Prefix_SeekAfetrFlush --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_SeekAfterFlush){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.iterate_upper_bound = nullptr;
 
    DestroyAndReopen(options);
    ASSERT_OK(db_->Put(wropt, "ab", "value1"));
    ASSERT_OK(db_->Put(wropt, "cd", "value2"));
    ASSERT_OK(db_->Put(wropt, "fd", "value3"));
    ASSERT_OK(db_->Put(wropt, "123","value4"));
    ASSERT_OK(db_->Put(wropt, "fg", "value5"));
    ASSERT_OK(Flush());
    auto iter = db_->NewIterator(ropt);
    
    iter ->Seek("f00");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("fd",iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("fg",iter->key().ToString());
    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
    }
/**
 * @brief         Use InDomainTest test Prefix domain function
 * @param         PrefixTest --- Class name. \n
 *                InDomainTest --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, InDomainTest) {
    //options.comparator = 
    options.prefix_extractor.reset(new SamePrefixTransform("HHKB"));
    //BlockBasedTableOptions bbto;
    //bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
    //bbto.whole_key_filtering = false;
    //options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    
    {
        DestroyAndReopen(options);
        ASSERT_OK(db_->Put(wropt, "HHKB pro2", "Mar 24, 2018"));
        ASSERT_OK(db_->Put(wropt, "HHKB pro2 Type-S", "June 29, 2019"));
        ASSERT_OK(db_->Put(wropt, "Realforce 87u", "idk"));
        //db_->Flush(FlushOptions());
        //std::string result;
        auto db_iter = db_->NewIterator(ReadOptions());

        db_iter->Seek("Realforce 87u");
        ASSERT_TRUE(db_iter->Valid());
        ASSERT_OK(db_iter->status());
        ASSERT_EQ(db_iter->key(), "Realforce 87u");
        ASSERT_EQ(db_iter->value(), "idk");

        delete db_iter;
        //delete db_;
    }

    {
        cout<<"start to test"<<endl;
        DestroyAndReopen(options);
        ASSERT_OK(db_->Put(wropt, "pikachu", "1"));
        ASSERT_OK(db_->Put(wropt, "Meowth", "1"));
        ASSERT_OK(db_->Put(wropt, "Mewtwo", "idk"));
        //db_->Flush(FlushOptions());
        //std::string result;
        auto db_iter = db_->NewIterator(ReadOptions());
        cout<<"start to seek"<<endl;
        db_iter->Seek("Mewtwo");
        ASSERT_TRUE(db_iter->Valid());
        ASSERT_OK(db_iter->status());
        delete db_iter;
        //delete db_;
    }
}
/**
 * @brief         Use TestResult test some scenario prefix  function
 * @param         PrefixTest --- Class name. \n
 *                TestResult --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, TestResult) {
    
    for (int num_buckets = 1; num_buckets <= 2; num_buckets++) {
        FirstOption();
        while (NextOptions(num_buckets)) {
/*             std::cout << "*** Mem table: " << options.memtable_factory->Name()
                                << " number of buckets: " << num_buckets
                                << std::endl; */
            DestroyAndReopen(options);
            ReadOptions ropt;

            // 1. Insert one row.
            Slice v16("v16");
            PutKey(db_, wropt, 1, 6, v16);
            std::unique_ptr<Iterator> iter(db_->NewIterator(ropt));
            SeekIterator(iter.get(), 1, 6);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v16 == iter->value());
            SeekIterator(iter.get(), 1, 5);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v16 == iter->value());
            SeekIterator(iter.get(), 1, 5);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v16 == iter->value());
            iter->Next();
            ASSERT_TRUE(!iter->Valid());

            SeekIterator(iter.get(), 2, 0);
            ASSERT_TRUE(!iter->Valid());

            ASSERT_EQ(v16.ToString(), GetKey(db_, ropt, 1, 6));
            ASSERT_EQ(kNotFoundResult, GetKey(db_, ropt, 1, 5));
            ASSERT_EQ(kNotFoundResult, GetKey(db_, ropt, 1, 7));
            ASSERT_EQ(kNotFoundResult, GetKey(db_, ropt, 0, 6));
            ASSERT_EQ(kNotFoundResult, GetKey(db_, ropt, 2, 6));

            // 2. Insert an entry for the same prefix as the last entry in the bucket.
            Slice v17("v17");
            PutKey(db_, wropt, 1, 7, v17);
            iter.reset(db_->NewIterator(ropt));
            SeekIterator(iter.get(), 1, 7);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v17 == iter->value());

            SeekIterator(iter.get(), 1, 6);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v16 == iter->value());
            iter->Next();
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v17 == iter->value());
            iter->Next();
            ASSERT_TRUE(!iter->Valid());

            SeekIterator(iter.get(), 2, 0);
            ASSERT_TRUE(!iter->Valid());

            // 3. Insert an entry for the same prefix as the head of the bucket.
            Slice v15("v15");
            PutKey(db_, wropt, 1, 5, v15);
            iter.reset(db_->NewIterator(ropt));

            SeekIterator(iter.get(), 1, 7);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v17 == iter->value());

            SeekIterator(iter.get(), 1, 5);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v15 == iter->value());
            iter->Next();
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v16 == iter->value());
            iter->Next();
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v17 == iter->value());

            SeekIterator(iter.get(), 1, 5);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v15 == iter->value());

            ASSERT_EQ(v15.ToString(), GetKey(db_, ropt, 1, 5));
            ASSERT_EQ(v16.ToString(), GetKey(db_, ropt, 1, 6));
            ASSERT_EQ(v17.ToString(), GetKey(db_, ropt, 1, 7));

            // 4. Insert an entry with a larger prefix
            Slice v22("v22");
            PutKey(db_, wropt, 2, 2, v22);
            iter.reset(db_->NewIterator(ropt));

            SeekIterator(iter.get(), 2, 2);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v22 == iter->value());
            SeekIterator(iter.get(), 2, 0);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v22 == iter->value());

            SeekIterator(iter.get(), 1, 5);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v15 == iter->value());

            SeekIterator(iter.get(), 1, 7);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v17 == iter->value());

            // 5. Insert an entry with a smaller prefix
            Slice v02("v02");
            PutKey(db_, wropt, 0, 2, v02);
            iter.reset(db_->NewIterator(ropt));

            SeekIterator(iter.get(), 0, 2);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v02 == iter->value());
            SeekIterator(iter.get(), 0, 0);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v02 == iter->value());

            SeekIterator(iter.get(), 2, 0);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v22 == iter->value());

            SeekIterator(iter.get(), 1, 5);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v15 == iter->value());

            SeekIterator(iter.get(), 1, 7);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v17 == iter->value());

            // 6. Insert to the beginning and the end of the first prefix
            Slice v13("v13");
            Slice v18("v18");
            PutKey(db_, wropt, 1, 3, v13);
            PutKey(db_, wropt, 1, 8, v18);
            iter.reset(db_->NewIterator(ropt));
            SeekIterator(iter.get(), 1, 7);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v17 == iter->value());

            SeekIterator(iter.get(), 1, 3);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v13 == iter->value());
            iter->Next();
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v15 == iter->value());
            iter->Next();
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v16 == iter->value());
            iter->Next();
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v17 == iter->value());
            iter->Next();
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v18 == iter->value());

            SeekIterator(iter.get(), 0, 0);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v02 == iter->value());

            SeekIterator(iter.get(), 2, 0);
            ASSERT_TRUE(iter->Valid());
            ASSERT_TRUE(v22 == iter->value());

            ASSERT_EQ(v22.ToString(), GetKey(db_, ropt, 2, 2));
            ASSERT_EQ(v02.ToString(), GetKey(db_, ropt, 0, 2));
            ASSERT_EQ(v13.ToString(), GetKey(db_, ropt, 1, 3));
            ASSERT_EQ(v15.ToString(), GetKey(db_, ropt, 1, 5));
            ASSERT_EQ(v16.ToString(), GetKey(db_, ropt, 1, 6));
            ASSERT_EQ(v17.ToString(), GetKey(db_, ropt, 1, 7));
            ASSERT_EQ(v18.ToString(), GetKey(db_, ropt, 1, 8));
        }
    }
}
/**
 * @brief         Use PrefixValid test some scenario prefix  function
 * @param         PrefixTest --- Class name. \n
 *                PrefixValid --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */
// Show results in prefix
TEST_F(PrefixTest, PrefixValid) {
    //for (int num_buckets = 1; num_buckets <= 2; num_buckets++) {
        //FirstOption();
     // while (NextOptions(num_buckets)) {
         // std::cout << "*** Mem table: " << options.memtable_factory->Name()
         //                     << " number of buckets: " << num_buckets << std::endl;
    DestroyAndReopen(options);

    // Insert keys with common prefix and one key with different
    Slice v16("v16");
    Slice v17("v17");
    Slice v18("v18");
    Slice v19("v19");
    PutKey(db_, wropt, 12345, 6, v16);
    PutKey(db_, wropt, 12345, 7, v17);
    PutKey(db_, wropt, 12345, 8, v18);
    PutKey(db_, wropt, 12345, 9, v19);
    PutKey(db_, wropt, 12346, 8, v16);
    //db_->Flush(FlushOptions());
    TestKey test_key(12346, 8);
    std::string s;
    db_->Delete(wropt, TestKeyToSlice(s, test_key));
    //db_->Flush(FlushOptions());
    ropt.prefix_same_as_start = true;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ropt));
    SeekIterator(iter.get(), 12345, 6);
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(v16 == iter->value());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(v17 == iter->value());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(v18 == iter->value());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(v19 == iter->value());
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_EQ(kNotFoundResult, GetKey(db_, ropt, 12346, 8));

    // Verify seeking past the prefix won't return a result.
    SeekIterator(iter.get(), 12345, 10);
    ASSERT_TRUE(!iter->Valid());
// }
//}
}
/**
 * @brief         Use DynamicPrefixIterator test Dynamic prefix  function
 * @param         PrefixTest --- Class name. \n
 *                DynamicPrefixIterator --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 *
 */

TEST_F(PrefixTest, DynamicPrefixIterator) {
    DestroyAndReopen(options);
    while (NextOptions(100000)) {
        std::cout << "*** Mem table: " << options.memtable_factory->Name()<< std::endl; 
        Reopen(options);

        std::vector<uint64_t> prefixes;
        for (uint64_t i = 0; i < 10000; ++i) {
            prefixes.push_back(i);
        }

         if (false) {
            std::random_shuffle(prefixes.begin(), prefixes.end());
        }

        HistogramImpl hist_put_time;
        HistogramImpl hist_put_comparison;

        // insert x random prefix, each with y continuous element.
        for (auto prefix : prefixes) {
             for (uint64_t sorted = 0; sorted < 1; sorted++) {
                TestKey test_key(prefix, sorted);

                std::string s;
                Slice key = TestKeyToSlice(s, test_key);
                std::string value(40, 0);

                get_perf_context()->Reset();
                StopWatchNano timer(Env::Default(), true);
                ASSERT_OK(db_->Put(wropt, key, value));
                hist_put_time.Add(timer.ElapsedNanos());
                hist_put_comparison.Add(get_perf_context()->user_key_comparison_count);
            }
        }

         std::cout << "Put key comparison: \n" << hist_put_comparison.ToString()
                            << "Put time: \n" << hist_put_time.ToString(); 

        // test seek existing keys
        HistogramImpl hist_seek_time;
        HistogramImpl hist_seek_comparison;

        std::unique_ptr<Iterator> iter(db_->NewIterator(ropt));

        for (auto prefix : prefixes) {
            TestKey test_key(prefix, 1 / 2);
            std::string s;
            Slice key = TestKeyToSlice(s, test_key);
            std::string value = "v" + ToString(0);

            get_perf_context()->Reset();
            StopWatchNano timer(Env::Default(), true);
            auto key_prefix = options.prefix_extractor->Transform(key);
            uint64_t total_keys = 0;
            for (iter->Seek(key);
                     iter->Valid() && iter->key().starts_with(key_prefix);
                     iter->Next()) {
                if (false) {
                   // std::cout << "Behold the deadlock!\n";
                    db_->Delete(wropt, iter->key());
                }
                total_keys++;
            }
            hist_seek_time.Add(timer.ElapsedNanos());
            hist_seek_comparison.Add(get_perf_context()->user_key_comparison_count);
            ASSERT_EQ(total_keys, 1 - 1/2);
        }

         std::cout << "Seek key comparison: \n"
                            << hist_seek_comparison.ToString()
                            << "Seek time: \n"
                            << hist_seek_time.ToString(); 

        // test non-existing keys
        HistogramImpl hist_no_seek_time;
        HistogramImpl hist_no_seek_comparison;

        for (auto prefix = 100000;
                 prefix < 100000 + 10000;
                 prefix++) {
            TestKey test_key(prefix, 0);
            //cout<<prefix<<endl;
            std::string s;
            Slice key = TestKeyToSlice(s, test_key);
            get_perf_context()->Reset();
            StopWatchNano timer(Env::Default(), true);
            iter->Seek("@#$");
            //cout<<iter->key().ToString()<<endl;
            hist_no_seek_time.Add(timer.ElapsedNanos());
            hist_no_seek_comparison.Add(get_perf_context()->user_key_comparison_count);
            ASSERT_TRUE(iter->Valid());
        }

         std::cout << "non-existing Seek key comparison: \n"
                            << hist_no_seek_comparison.ToString()
                            << "non-existing Seek time: \n"
                            << hist_no_seek_time.ToString(); 
    }
}

/**
 * @brief         Use PrefixSeekModePrev test prefix seek mode function
 * @param         PrefixTest --- Class name. \n
 *                PrefixSeekModePrev --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */

TEST_F(PrefixTest, PrefixSeekModePrev) {
    // Only for SkipListFactory
    options.memtable_factory.reset(new SkipListFactory);
    options.merge_operator = MergeOperators::CreatePutOperator();
    options.write_buffer_size = 1024 * 1024;
    Random rnd(1);
	DestroyAndReopen(options);
    for (size_t m = 1; m < 100; m++) {
        /*std::cout << "[" + std::to_string(m) + "]" + "*** Mem table: "
                            << options.memtable_factory->Name() << std::endl;*/
		Reopen(options);
        std::map<TestKey, std::string, TestKeyComparator> entry_maps[3], whole_map;
        for (uint64_t i = 0; i < 10; i++) {
            int div = i % 3 + 1;
            for (uint64_t j = 0; j < 10; j++) {
                whole_map[TestKey(i, j)] = entry_maps[rnd.Uniform(div)][TestKey(i, j)] =
                        'v' + std::to_string(i) + std::to_string(j);
            }
        }

        std::map<TestKey, std::string, TestKeyComparator> type_map;
        for (size_t i = 0; i < 3; i++) {
            for (auto& kv : entry_maps[i]) {
                if (rnd.OneIn(3)) {
                    PutKey(db_, wropt, kv.first, kv.second);
                    type_map[kv.first] = "value";
                } else {
                    MergeKey(db_, wropt, kv.first, kv.second);
                    type_map[kv.first] = "merge";
                }
            }
            //if (i < 2) {
            //    db_->Flush(FlushOptions());
            //}
        }

        for (size_t i = 0; i < 2; i++) {
            for (auto& kv : entry_maps[i]) {
                if (rnd.OneIn(10)) {
                    whole_map.erase(kv.first);
                    DeleteKey(db_, wropt, kv.first);
                    entry_maps[2][kv.first] = "delete";
                }
            }
        }
            for (size_t i = 0; i < 3; i++) {
                for (auto& kv : entry_maps[i]) {
                    std::cout << "[" << i << "]" << kv.first.prefix << kv.first.sorted
                                        << " " << kv.second + " " + type_map[kv.first] << std::endl;
                }
            }

        std::unique_ptr<Iterator> iter(db_->NewIterator(ropt));
        for (uint64_t prefix = 0; prefix < 10; prefix++) {
            uint64_t start_suffix = rnd.Uniform(9);
            SeekIterator(iter.get(), prefix, start_suffix);
            auto it = whole_map.find(TestKey(prefix, start_suffix));
            if (it == whole_map.end()) {
                continue;
            }
            ASSERT_NE(it, whole_map.end());
            ASSERT_TRUE(iter->Valid());
/*             std::cout << "round " << prefix
                                    << " iter: " << SliceToTestKey(iter->key()).prefix
                                    << SliceToTestKey(iter->key()).sorted
                                    << " | map: " << it->first.prefix << it->first.sorted << " | "
                                    << iter->value().ToString() << " " << it->second << std::endl; */
            ASSERT_EQ(iter->value(), it->second);
            uint64_t stored_prefix = prefix;
            for (size_t k = 0; k < 9; k++) {
                if (rnd.OneIn(2) || it == whole_map.begin()) {
                    iter->Next();
                    it++;
                        //std::cout << "Next >> ";
                } else {
                    iter->Prev();
                    it--;
                        //std::cout << "Prev >> ";
                }
                if (!iter->Valid() ||
                        SliceToTestKey(iter->key()).prefix != stored_prefix) {
                    break;
                }
                stored_prefix = SliceToTestKey(iter->key()).prefix;
                ASSERT_TRUE(iter->Valid());
                ASSERT_NE(it, whole_map.end());
                ASSERT_EQ(iter->value(), it->second);
/*                     std::cout << "iter: " << SliceToTestKey(iter->key()).prefix
                                        << SliceToTestKey(iter->key()).sorted
                                        << " | map: " << it->first.prefix << it->first.sorted
                                        << " | " << iter->value().ToString() << " " << it->second
                                        << std::endl; */
            }
        }
    }
}

/**
 * @brief         Use PrefixSeekModePrev2 test prefix seek mode function
 * @param         PrefixTest --- Class name. \n
 *                PrefixSeekModePrev2 --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, PrefixSeekModePrev2) {
    // Only for SkipListFactory
    // test the case
    //                iter1                                iter2
    // | prefix | suffix |    | prefix | suffix |
    // |     1        |     1        |    |     1        |     2        |
    // |     1        |     3        |    |     1        |     4        |
    // |     2        |     1        |    |     3        |     3        |
    // |     2        |     2        |    |     3        |     4        |
    // after seek(15), iter1 will be at 21 and iter2 will be 33.
    // Then if call Prev() in prefix mode where SeekForPrev(21) gets called,
    // iter2 should turn to invalid state because of bloom filter.
    options.memtable_factory.reset(new SkipListFactory);
    options.write_buffer_size = 1024 * 1024;
    DestroyAndReopen(options);
    PutKey(db_, wropt, TestKey(1, 2), "v12");
    PutKey(db_, wropt, TestKey(1, 4), "v14");
    PutKey(db_, wropt, TestKey(3, 3), "v33");
    PutKey(db_, wropt, TestKey(3, 4), "v34");
    db_->Flush(FlushOptions());
    PutKey(db_, wropt, TestKey(1, 1), "v11");
    PutKey(db_, wropt, TestKey(1, 3), "v13");
    PutKey(db_, wropt, TestKey(2, 1), "v21");
    PutKey(db_, wropt, TestKey(2, 2), "v22");
    db_->Flush(FlushOptions());
    std::unique_ptr<Iterator> iter(db_->NewIterator(ropt));
    SeekIterator(iter.get(), 1, 5);
    iter->Prev();
    ASSERT_EQ(iter->value().ToString(), "v14");
}
/**
 * @brief         Use PrefixSeekModePrev2 test prefix seek upper_bound mode function
 * @param         PrefixTest --- Class name. \n
 *                PrefixSeekModePrev2 --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */
//For Test upper_bound
TEST_F(PrefixTest, PrefixSeekModePrev3) {
    // Only for SkipListFactory
    // test SeekToLast() with iterate_upper_bound_ in prefix_seek_mode
    options.memtable_factory.reset(new SkipListFactory);
    options.write_buffer_size = 1024 * 1024;
    std::string v14("v14");
    TestKey upper_bound_key = TestKey(1, 5);
    std::string s;
    Slice upper_bound = TestKeyToSlice(s, upper_bound_key);

    {
        DestroyAndReopen(options);
        ropt.iterate_upper_bound = &upper_bound;
        PutKey(db_, wropt, TestKey(1, 2), "v12");
        PutKey(db_, wropt, TestKey(1, 4), "v14");
        db_->Flush(FlushOptions());
        PutKey(db_, wropt, TestKey(1, 1), "v11");
        PutKey(db_, wropt, TestKey(1, 3), "v13");
        PutKey(db_, wropt, TestKey(2, 1), "v21");
        PutKey(db_, wropt, TestKey(2, 2), "v22");
        db_->Flush(FlushOptions());
        std::unique_ptr<Iterator> iter(db_->NewIterator(ropt));
        iter->SeekToLast();
        ASSERT_EQ(iter->value().ToString(), v14);
    }
    {
        DestroyAndReopen(options);
        ropt.iterate_upper_bound = &upper_bound;
        PutKey(db_, wropt, TestKey(1, 2), "v12");
        PutKey(db_, wropt, TestKey(1, 4), "v14");
        PutKey(db_, wropt, TestKey(3, 3), "v33");
        PutKey(db_, wropt, TestKey(3, 4), "v34");
        db_->Flush(FlushOptions());
        PutKey(db_, wropt, TestKey(1, 1), "v11");
        PutKey(db_, wropt, TestKey(1, 3), "v13");
        db_->Flush(FlushOptions());
        std::unique_ptr<Iterator> iter(db_->NewIterator(ropt));
        iter->SeekToLast();
        ASSERT_EQ(iter->value().ToString(), v14);
    }
}
/**
 * @brief         Use Prefix_Upper test prefix seek more than upper bound 
 * @param         PrefixTest --- Class name. \n
 *                Prefix_Upper --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */
TEST_F(PrefixTest, Prefix_Upper){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.prefix_same_as_start = false;
    Slice upper("dd");
    ropt.iterate_upper_bound = &upper;
    
    
    cout<<"DB is open"<<endl;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "aa", "1"));
    ASSERT_OK(db_->Put(wropt, "bb", "1"));
    ASSERT_OK(db_->Put(wropt, "cc", "1"));
    ASSERT_OK(db_->Put(wropt, "de", "3"));
	
    auto iter = db_->NewIterator(ropt);
    cout<<"start to seek"<<endl;
    iter -> Seek("de");
    ASSERT_TRUE(!iter->Valid());   
    delete iter;
  }
/**
 * @brief         Use Prefix_UpperWithNext test prefix upper bound with Next 
 * @param         PrefixTest --- Class name. \n
 *                Prefix_UpperWithNext --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */  
TEST_F(PrefixTest, Prefix_UpperWithNext){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.prefix_same_as_start = false;
	Slice upper("66");
    ropt.iterate_upper_bound = &upper;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "22", "1"));
    ASSERT_OK(db_->Put(wropt, "33", "1"));
    ASSERT_OK(db_->Put(wropt, "65", "1"));
    ASSERT_OK(db_->Put(wropt, "67", "3"));
	
    auto iter = db_->NewIterator(ropt);
    iter -> Seek("30");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("33",iter->key().ToString()); 
    iter -> Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("65",iter->key().ToString());
    iter -> Next();
    ASSERT_TRUE(!iter->Valid()); 	
    delete iter;
	
}
/**
 * @brief         Use Prefix_UpperWithPrev test prefix upper bound with Prev 
 * @param         PrefixTest --- Class name. \n
 *                Prefix_UpperWithPrev --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */ 
TEST_F(PrefixTest, Prefix_UpperWithPrev){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.prefix_same_as_start = false;
	Slice upper("66");
    ropt.iterate_upper_bound = &upper;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "22", "1"));
    ASSERT_OK(db_->Put(wropt, "33", "1"));
    ASSERT_OK(db_->Put(wropt, "65", "1"));
    ASSERT_OK(db_->Put(wropt, "67", "3"));
	
    auto iter = db_->NewIterator(ropt);
    iter -> SeekToLast();
	ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("65",iter->key().ToString()); 
    iter -> Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("33",iter->key().ToString());  
    delete iter;
}
/**
 * @brief         Use Prefix_UpperToFirst test prefix upper bound with SeekToFirst 
 * @param         PrefixTest --- Class name. \n
 *                Prefix_UpperToFirst --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */ 
TEST_F(PrefixTest, Prefix_UpperToFirst){
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    ropt.prefix_same_as_start = false;
	Slice upper("15");
    ropt.iterate_upper_bound = &upper;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "22", "1"));
    ASSERT_OK(db_->Put(wropt, "33", "1"));
    ASSERT_OK(db_->Put(wropt, "65", "1"));
    ASSERT_OK(db_->Put(wropt, "67", "3"));
	
    auto iter = db_->NewIterator(ropt);
    iter -> SeekToFirst();
	ASSERT_TRUE(!iter->Valid());
    delete iter;
}
/**
 * @brief         Use Prefix_UpperToFirst test prefix as same as start 
 * @param         PrefixTest --- Class name. \n
 *                Prefix_UpperToFirst --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */ 
 TEST_F(PrefixTest, PrefixSame_Upper){
    options.prefix_extractor.reset(NewFixedPrefixTransform(3));
    ropt.prefix_same_as_start = true;
	Slice upper("ddf");
    ropt.iterate_upper_bound = &upper;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "abc", "1"));
    ASSERT_OK(db_->Put(wropt, "dcf", "1"));
    ASSERT_OK(db_->Put(wropt, "ddf", "1"));
    ASSERT_OK(db_->Put(wropt, "ddg", "3"));
	
    auto iter = db_->NewIterator(ropt);
    iter -> Seek("ddg");
	ASSERT_TRUE(!iter->Valid());
    delete iter;
}
/**
 * @brief         Use PrefixSame_UpperNext test prefix as same as start and Next seek
 * @param         PrefixTest --- Class name. \n
 *                PrefixSame_UpperNext --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */ 
 TEST_F(PrefixTest, PrefixSame_UpperNext){
    options.prefix_extractor.reset(NewFixedPrefixTransform(2));
    ropt.prefix_same_as_start = true;
	Slice upper("abe");
    ropt.iterate_upper_bound = &upper;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "abc", "1"));
    ASSERT_OK(db_->Put(wropt, "dcf", "1"));
    ASSERT_OK(db_->Put(wropt, "def", "1"));
    ASSERT_OK(db_->Put(wropt, "dfg", "3"));
	
    auto iter = db_->NewIterator(ropt);
    iter -> Seek("abb");
	ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("abc",iter->key().ToString());
    iter -> Next();
	ASSERT_TRUE(!iter->Valid());
    delete iter;
}
/**
 * @brief         Use PrefixSame_UpperPrev test prefix as same as start and Prev seek
 * @param         PrefixTest --- Class name. \n
 *                PrefixSame_UpperPrev --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */ 
TEST_F(PrefixTest, PrefixSame_UpperPrev){
    options.prefix_extractor.reset(NewFixedPrefixTransform(2));
    ropt.prefix_same_as_start = false;
	Slice upper("669");
    ropt.iterate_upper_bound = &upper;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "222", "1"));
    ASSERT_OK(db_->Put(wropt, "456", "1"));
    ASSERT_OK(db_->Put(wropt, "668", "3"));
    ASSERT_OK(db_->Put(wropt, "678", "3"));
	
    auto iter = db_->NewIterator(ropt);
    iter -> SeekToLast();
	ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("668",iter->key().ToString()); 
    iter -> Seek("665");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("668",iter->key().ToString());  
    delete iter;
}
/**
 * @brief         Use PrefixSame_UpperToFirst test same prefix upper bound with SeekToFirst 
 * @param         PrefixTest --- Class name. \n
 *                PrefixSame_UpperToFirst --- Function name.
 * @return        void
 * @author        RocksDB
 * @version       v0.1
 * @remark        
 */ 
TEST_F(PrefixTest, PrefixSame_UpperToFirst){
    options.prefix_extractor.reset(NewFixedPrefixTransform(2));
    ropt.prefix_same_as_start = true;
	Slice upper("455");
    ropt.iterate_upper_bound = &upper;
    DestroyAndReopen(options);
    cout<<"start to put data in to DB"<<endl;
    ASSERT_OK(db_->Put(wropt, "145", "1"));
    ASSERT_OK(db_->Put(wropt, "333", "1"));
    ASSERT_OK(db_->Put(wropt, "65", "1"));
    ASSERT_OK(db_->Put(wropt, "67", "3"));
	
    auto iter = db_->NewIterator(ropt);
    iter -> SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("145",iter->key().ToString()); 
	iter -> Seek("332");
    ASSERT_TRUE(iter->Valid());
	ASSERT_EQ("333",iter->key().ToString()); 
    iter -> Next();
    ASSERT_TRUE(!iter->Valid());
    delete iter;
}
}
