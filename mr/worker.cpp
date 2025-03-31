#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <string.h>
#include <signal.h>
#include <sys/time.h>
#include "locker.h"
#include <bits/stdc++.h>
#include <pthread.h>
#include <dlfcn.h>
#include "../buttonrpc-master/buttonrpc.hpp"

using namespace std;

#define MAX_REDUCE_NUM 15

class KeyValue {
public:
    string key;
    string value;
};

pthread_mutex_t map_mutex;  // map互斥锁
pthread_cond_t cond;  // 条件变量

typedef vector<KeyValue> (*MapFunc)(KeyValue kv);
typedef vector<string> (*ReduceFunc)(vector<KeyValue> kvs, int reduceTaskIdx);
/*
    typedef可以自定义新的数据类型，此处的语法是自定义一个函数指针：
    vector<KeyValue> - 即函数返回的类型；
    (*MapFunc) -       自定义函数名；
    (KeyValue kv) -    函数接收的参数类型。
*/
MapFunc mapF;
ReduceFunc reduceF;

int map_task_num;
int reduce_task_num = 2;

int MapId = 0;  // map worker的ID

int ihash(string strs) {
    int sum = 0;
    for (int i = 0; i < strs.size(); i++) {
        sum += (strs[i] - '0');
    }
    return sum % reduce_task_num;
}

// 根据任务名获取文件内容
void getcontent(string task, KeyValue& kv) {
    ifstream fin;
    string path = "../data/pg-" + task + ".txt";
    fin.open(path, ios::in);
    if (!fin) {
        cerr << "Cannot open file" << task << endl;
        exit(-1);
    }
    string content;
    char c;
    while ((c = fin.get()) != EOF) {
        content += c;
    }
    kv.key = task;
    kv.value = content;
    fin.close();
}

void writekv(int fd, KeyValue kv) {
    string str = kv.key + ",1 ";
    int len = write(fd, str.c_str(), str.size());
    if (len == -1) {
        perror("write: ");  // 能够将上一个函数的错误信息输出
        exit(-1);
    }
    close(fd);
}

void writeIntermFile(vector<KeyValue> kvs, int mapTaskIdx) {
    for (const auto& kv: kvs) {
        int reduceIdx = ihash(kv.key);
        string path = "../data/interm-" + to_string(mapTaskIdx) + '-' + to_string(reduceIdx);
        int ret = access(path.c_str(), F_OK);
        if (ret == 0) {
            int fd = open(path.c_str(), O_WRONLY | O_APPEND);
            writekv(fd, kv);
        } else if (ret == -1) {
            int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0664);
            writekv(fd, kv);
        }

    }
}

// mapworker
void* mapWorker(void* arg) {  // void*定义的函数可以返回任意类型的指针
    // 1. map worker进程初始化
    buttonrpc client;
    client.as_client("127.0.0.1", 5555);
    pthread_mutex_lock(&map_mutex);
    int mapTaskIdx = MapId++;
    pthread_mutex_unlock(&map_mutex);
    bool ret = false;

    // 实际执行map的部分
    while(1) {
        // 2. 通过master获取任务
        ret = client.call<bool>("isMapDone").val();
        if (ret) {           // 终止线程条件：所有文件都已完成map
            pthread_cond_broadcast(&cond);
            return NULL;
        }
        string taskTmp = client.call<string>("assignTask").val();   //通过RPC返回值取得任务，在map中即为文件名
        printf("map worker %d got task %s\n", mapTaskIdx, taskTmp.c_str());
         
        if (taskTmp == "empty") continue;

        // 3. 获取字符串内容
        KeyValue kv;
        getcontent(taskTmp, kv);

        // 4. map运算进行，写入磁盘
        vector<KeyValue> kvs = mapF(kv);
        writeIntermFile(kvs, mapTaskIdx);

        // 5. 调用rpc通知master当前map已完成
        printf("map worker %d finished task %s", mapTaskIdx, taskTmp.c_str());
        if(client.call<bool>("setMapStat").val()) {

        }
    }
}

// 删除所有写入中间值的临时文件
void removeFiles(){
    string path;
    for(int i = 0; i < map_task_num; i++){
        for(int j = 0; j < reduce_task_num; j++){
            path = "mr-" + to_string(i) + "-" + to_string(j);
            int ret = access(path.c_str(), F_OK);
            if(ret == 0) remove(path.c_str());
        }
    }
}

// 删除上一次的输出文件
void removeOutputFiles(){
    string path;
    for(int i = 0; i < MAX_REDUCE_NUM; i++){
        path = "mr-out-" + to_string(i);
        int ret = access(path.c_str(), F_OK);
        if(ret == 0) remove(path.c_str());
    }
}

int main() {

    pthread_mutex_init(&map_mutex, NULL);
    // PTHREAD_MUTEX_TIMED_NP，这是第二位参数的缺省值，也就是普通锁。当一个线程加锁以后，
    // 其余请求锁的线程将形成一个等待队列，并在解锁后按优先级获得锁。这种锁策略保证了资源分配的公平性。
    pthread_cond_init(&cond, NULL);
    // 第二位参数缺省默认条件变量只能被进程内部的线程使用

    void* handle = dlopen("../obj/libmrFunc.so", RTLD_LAZY);
    /*
        这里涉及到几个知识点：
        1. 首先是void*，void*作为无类型指针与int*，float*等有类型指针最大的区别在于void*可以不经过强制
            类型转换就可以被赋其它类型的值，比如dlopen返回值可能是有类型的，比如int*，但是不需要通过
            (int *)这种强制类型转换的语法就能赋值。
        2. dlopen用于从以指定模式打开指定的动态连接库文件，并返回一个句柄给调用进程。
            使用dlclose()可以卸载打开的库。
        3. .so文件属于动态库，可以以dlopen的方式显示地在c++中调用，相比于静态库的优势在于：静态库需要在
            编译后与程序进行链接后再运行；而动态库不需要与程序进行链接，而是在程序运行时被载入。所以当静态
            库代码更新时，需要对整个项目进行重新编译，动态库则可以直接运行增量更新，提高效率。另外，静态库
            由于存在多个拷贝及中间文件也存在占用内存过大的问题。
    */
    if (!handle) {
        cerr << "Cannot open library: " << dlerror() << "\n";
        exit(-1);
    }
    mapF = (MapFunc)dlsym(handle, "mapF");  // dlsym根据函数名返回void*类型函数指针，需要使用强制类型转换
    if (!mapF) {
        cerr << "Cannot load symbol 'hello': " << dlerror() <<'\n';
        dlclose(handle);
        exit(-1);
    }
    reduceF = (ReduceFunc)dlsym(handle, "reduceF");
    if (!reduceF) {
        cerr << "Cannot load symbol 'hello': " << dlerror() <<'\n';
        dlclose(handle);
        exit(-1);
    }

    buttonrpc work_client;
    work_client.as_client("127.0.0.1", 5555);
    work_client.set_timeout(5000);

    // ----- 测试程序 ------
    KeyValue kv;
    getcontent("being_ernest", kv);
    vector<KeyValue> kvs = mapF(kv);
    writeIntermFile(kvs, 0);
    // ----- 测试程序 ------

    // map_task_num = work_client.call<int>("getMapNum").val();
    // reduce_task_num = work_client.call<int>("getReduceNum").val();
    // cout << "Done!" << endl;

    // // TODO：删除上次写入的文件
    // removeFiles();
    // removeOutputFiles();

    // //创建多个map及reduce的worker线程
    // pthread_t tidMap[map_task_num];       // pthread_t为unsigned long int，用于定义线程编号
    // pthread_t tidReduce[reduce_task_num];
    // for (int i = 0; i < map_task_num; i++) {
    //     pthread_create(&tidMap[i], NULL, mapWorker, NULL);  // 创建线程，赋予线程ID以及线程对应的函数
    //     pthread_detach(tidMap[i]);                          // 使进程在结束后主动释放资源，与主控进程断开关系
    // }
    return 0;
}