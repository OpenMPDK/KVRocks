/*! \file kvdb_getApproximateSize.cc
 *  \brief Test cases code of the KVDB ApproximateSize feature.
*/
#include "kvdb_test.h"

namespace rocksdb {

class ApproximateSizeTest: public KVDBTest {
public:
    std::string start;
    std::string end;
    uint64_t size = 0;
    Range r;
    uint8_t include_both = DB::SizeApproximationFlags::INCLUDE_FILES |
                           DB::SizeApproximationFlags::INCLUDE_MEMTABLES; 

public:
    ApproximateSizeTest() {
        dbname_ = test::TmpDir() + "/approximatesize_test";
        db_options_.create_if_missing = true;
        db_options_.fail_if_options_file_error = true;
        DestroyDB(dbname_, Options(db_options_, column_family_options_));
    }


    ~ApproximateSizeTest() {
        Destroy();
    }
};
/********************************************************************************************/
/******************************************Test cases****************************************/
/********************************************************************************************/
/*
 * @brief         Use ApproximateSizes_Smallrange to small range like range = 1
 * @param         ApproximateSizeTest --- Class name. \n
 *                ApproximateSizes_Smallrange --- Function name.
 * @return        void
 * @author        Liu Liu(ll66.liu@sumsung.com)
 * @version       v0.1
 * @remark        
 */
TEST_F(ApproximateSizeTest,ApproximateSizes_Smallrange) {
    Open();
    ASSERT_OK(Put("a","1"));
    ASSERT_OK(Put("b","2"));
    start = "a";
    end = "b";
    r = Range(start,end);
    cout<<start<<"~"<<end<<endl;
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"size is "<<size<<endl;
    ASSERT_GE(size,65535);
    Close();
}

TEST_F(ApproximateSizeTest,ApproximateSizes_range) {
    Open();
    const int N = 31000;
    for (int i = 0;i <= N;i++){
        Put(Key(i),"1");
    }
    sleep(5);
    start = Key(1);
    end = Key(N);
    r = Range(start,end);
    cout<<start<<"~"<<end<<endl;
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"size is "<<size<<endl;
    ASSERT_GE(size,N*(10+1));
    Close();
}

TEST_F(ApproximateSizeTest,ApproximateSizes_Samerange) {
    Open();
    Random rnd(316);
    const int N = 100000;
    for (int i = 0;i <= N;i++){
        Put(Key(i),RandomString(&rnd,100));
    }
    sleep(5);
    for (int i = 1;i <= 10;i++){
        start = Key((i-1)*10000);
        end = Key(i*10000);
        r = Range(start,end);
        cout<<start<<"~"<<end<<endl;
        db_->GetApproximateSizes(&r, 1, &size, include_both);
        cout<<"size is "<<size<<endl;
        ASSERT_GE(size,10000*(10+100));
    }
    Close();
}
TEST_F(ApproximateSizeTest,ApproximateSizes_Increaserange) {
    Open();
    Random rnd(316);
    const int N = 1000000;
    for (int i = 0;i <= N;i++){
        Put(Key(i),RandomString(&rnd,100));
    }
    sleep(5);
    start = Key(0);
    for (int i = 1; i<= 10;i++){
        end = Key(i*100000);
        r = Range(start,end);
        cout<<start<<"~"<<end<<endl;
        db_->GetApproximateSizes(&r, 1, &size, include_both);
        cout<<"size is "<<size<<endl;
        ASSERT_GE(size,i*100000*(9+100));
    }
    Close();
}

TEST_F(ApproximateSizeTest, ApproximateSizes) {
    Options options;
    options.write_buffer_size = 100000000;    // Large write buffer
    options.compression = kNoCompression;
    Open();
    Random rnd(301);
    const int N = 128;
    for (int i = 0; i < N; i++) {
        ASSERT_OK(Put(Key(i), RandomString(&rnd, 1024)));
    }
    sleep(1);
    start = Key(50);
    end = Key(60);
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"The size is "<<size<<endl;
    ASSERT_GE(size, 10*(1024+10));

    for (int i = 0; i < N; i++) {
        ASSERT_OK(Put(Key(1000 + i), RandomString(&rnd, 1024)));
    }
    start = Key(500);
    end = Key(600);
    r = Range(start, end);
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"The size is "<<size<<endl;
    ASSERT_GE(size, 100*(1024+10));
    
    start = Key(100);
    end = Key(1020);
    r = Range(start, end);
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"The size is "<<size<<endl;
    ASSERT_GE(size, 920*(1024+10));

    int keys[N * 3];
    for (int i = 0; i < N; i++) {
        keys[i * 3] = i * 5;
        keys[i * 3 + 1] = i * 5 + 1;
        keys[i * 3 + 2] = i * 5 + 2;
    }
    std::random_shuffle(std::begin(keys), std::end(keys));
    for (int i = 0; i < N * 3; i++) {
        ASSERT_OK(Put(Key(keys[i] + 1000), RandomString(&rnd, 1024)));
    }
    start = Key(100);
    end = Key(300);
    r = Range(start, end);
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"The size is "<<size<<endl;
    ASSERT_GE(size, 200*(10+1024));

    start = Key(1050);
    end = Key(1080);
    r = Range(start, end);
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"The size is "<<size<<endl;
    ASSERT_GE(size, 30*(10+1024));

    start = Key(2100);
    end = Key(2300);
    r = Range(start, end);
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"The size is "<<size<<endl;
    ASSERT_GE(size, 200*(10+1024));

    start = Key(1050);
    end = Key(1080);
    r = Range(start, end);
    uint64_t size_with_mt, size_without_mt;
    db_->GetApproximateSizes(&r, 1, &size_without_mt);
    ASSERT_GT(size_without_mt, 30*(10+1024));
    //Flush(0);

    for (int i = 0; i < N; i++) {
        ASSERT_OK(Put(Key(i + 1000), RandomString(&rnd, 1024)));
    }

    start = Key(1050);
    end = Key(1080);
    r = Range(start, end);
    db_->GetApproximateSizes(&r, 1, &size_without_mt);
    ASSERT_GT(size_without_mt, 30*(10+1024));
    Close();
}

 // Now this feature can't supported CF
/*TEST_F(ApproximateSizeTest,ApproximateSizes_CF) {
    Options options;
    options.num_cols = 3;
    options.create_if_missing = true;
    db_options_=options;
    Open();
    CreateColumnFamiliesAndReopen({"66"});
 
    Random rnd(200);
    const int N = 1000000;
    for (int i = 0;i <= N;i++){
        Put(1,Key(i),RandomString(&rnd,100));
 }
 
 for (int i = 1;i <= 10;i++){
 start = Key((i-1)*100000);
 end = Key(i*100000);
 r = Range(start,end);
 cout<<start<<"~"<<end<<endl;
 db_->GetApproximateSizes(handles_[1],&r, 1, &size, include_both);
 cout<<"size is "<<size<<endl;
 ASSERT_GE(size, 100000*(100+10));
 }
}

TEST_F(ApproximateSizeTest,ApproximateSizes_DecreaseCF) {
    Options options;
    options.num_cols = 3;
    options.create_if_missing = true;
    db_options_=options;
    Open();
    CreateColumnFamiliesAndReopen({"66","77"});
    Random rnd(301);
    const int N = 1000000;
    for (int i = 0;i <= N;i++){
        Put(0,Key(i),RandomString(&rnd,16));
        Put(1,Key(i),RandomString(&rnd,100));
        Put(2,Key(i),RandomString(&rnd,110));
    }
 start = Key(0);
 for (int i = 1; i<= 10;i++){
 end = Key(i*100000);
 r = Range(start,end);
 cout<<start<<"~"<<end<<endl;
 db_->GetApproximateSizes(handles_[0], &r, 1, &size, include_both);
 cout<<"The cf 0 size is "<<size<<endl;
 db_->GetApproximateSizes(handles_[1], &r, 1, &size, include_both);
 cout<<"The cf 1 size is "<<size<<endl;
 db_->GetApproximateSizes(handles_[2], &r, 1, &size, include_both);
 cout<<"The cf 2 size is "<<size<<endl;
    }
 }
*/

TEST_F(ApproximateSizeTest, ApproximateSizes_DynamicRange) {
    Open();
    int num[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    Random rnd(301);
    const int N = 200000;
    const int a1 = 10000, a2 = 100000;
    for(int i = 0; i <= N; i++){
        Put(Key(i), RandomString(&rnd, 16));
    }
    sleep(1);
    uint64_t size;
    std::string    start = Key(num[0] * a1);
    std::string    end = Key(num[5] * a1);
    Range r(start, end); 
    cout<<start<<"~"<<end<<endl;        
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"size:"<<size<<endl;
    ASSERT_GE(size, 5*a1*(10+16));

    start = Key(num[5] * a1);
    end = Key(num[10] * a1);
    r = Range(start, end);
    cout<<start<<"~"<<end<<endl;
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"size:"<<size<<endl;
    ASSERT_GE(size, 5*a1*(10+16));

    start = Key(num[3] * a1);
    end = Key(num[8] * a1);
    r = Range(start, end);
    cout<<start<<"~"<<end<<endl;
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"size:"<<size<<endl;
    ASSERT_GE(size, 5*a1*(10+16));        

    start = Key(num[5] * a1);
    end = Key(num[6] * a1);
    r = Range(start, end);
    cout<<start<<"~"<<end<<endl;
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"size:"<<size<<endl;
    ASSERT_GE(size, a1*(10+16));

    start = Key(num[0] * a1);
    end = Key(num[10] * a1);
    r = Range(start, end);
    cout<<start<<"~"<<end<<endl;
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"size:"<<size<<endl;
    ASSERT_GE(size, 10*a1*(10+16));

    cout<<"############################################################"<<endl;
    for(long int i = N; i <= 10 * N; i++){
        Put(Key(i), RandomString(&rnd, 16));
    }
    start = Key(num[0] * a2);
    end = Key(num[5] * a2);    
    r = Range(start, end);
    cout<<start<<"~"<<end<<endl;        
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"size:"<<size<<endl;
    ASSERT_GE(size, 5*a2*(10+16));

    start = Key(num[5] * a2);
    end = Key(num[10] * a2);
    r = Range(start, end);
    cout<<start<<"~"<<end<<endl;
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"size:"<<size<<endl;
    ASSERT_GE(size, 5*a2*(10+16));

    start = Key(num[3] * a2);
    end = Key(num[8] * a2);
    r = Range(start, end);
    cout<<start<<"~"<<end<<endl;
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"size:"<<size<<endl;        
    ASSERT_GE(size, 5*a2*(10+16));

    start = Key(num[5] * a2);
    end = Key(num[6] * a2);
    r = Range(start, end);
    cout<<start<<"~"<<end<<endl;
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"size:"<<size<<endl;
    ASSERT_GE(size, 1*a2*(10+16));

    start = Key(num[0] * a2);
    end = Key(num[10] * a2);
    r = Range(start, end);
    cout<<start<<"~"<<end<<endl;
    db_->GetApproximateSizes(&r, 1, &size, include_both);
    cout<<"size:"<<size<<endl;
    ASSERT_GE(size, 10*a2*(10+16));
    Close();
}
}
