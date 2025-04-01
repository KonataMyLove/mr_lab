#include <iostream>
#include <string>
#include <vector>
#include <pthread.h>
#include <cstdlib>
#include <bits/stdc++.h>
#include "locker.h"
#include "../buttonrpc-master/buttonrpc.hpp"

using namespace std;


class Master {
public:
    static void* waitMapTask(void* arg);        // 回收map的定时线程
    // static修饰的类成员函数不属于实例对象本身，而属于类，无法访问非静态成员（即不带static修饰的成员）
    static void* waitTime(void* arg);
    Master(int mapNum = 2, int reduceNum = 2);  // 构造函数，带缺省值
    void GetAllFile(char* file[], int index);   // 将所有任务加入map任务工作队列
    int getMapNum() { return m_mapNum; }        // getter方法，可以访问类private属性
    int getReduceNum() { return m_reduceNum; }

    void waitMap(string filename);

    string assignTask();                        // 分配map任务的函数，RPC
    void setMapStat(string taskTmp);            // 设定map完成状态
    bool isMapDone();                           // 检查所有的map任务是否已经完成，RPC

private:
    list<char *> m_list;                        // map任务工作队列，是一个链表
    int fileNum;                                // 命令行读到的文件总数
    int m_mapNum;
    int m_reduceNum;
    locker m_assign_lock;                       // 共享数据保护锁，一个重新封装的类
    unordered_map<string, int> finishedMapTask; // 存放所有完成的map任务对应的文件名
    vector<string> runningMapWork;              // 正在处理的map任务，分配出去就加到这个队列，用于判断超时处理重发
};

Master::Master(int mapNum, int reduceNum): m_mapNum(mapNum), m_reduceNum(reduceNum) {  // 构造函数，实例化类对象时调用一次，可以用来初始化
    m_list.clear();
    finishedMapTask.clear();
    runningMapWork.clear();
    if (m_mapNum <= 0 || m_reduceNum <= 0) {
        throw std::exception();
    } 
    cout << "master successfully created and initialized" << endl;
}

void Master::GetAllFile(char* file[], int argc) {
    for (int i = 1; i < argc; i++) {  // 注意命令行传参数量是多少
        m_list.emplace_back(file[i]);
    }
    fileNum = argc - 1;
}

void* Master::waitTime(void* arg) {

}

void* Master::waitMapTask(void* arg) {
    Master* map = (Master*)arg;
    void* status;
    pthread_t tid;
    char op = 'm';
    pthread_create(&tid, NULL, waitTime, &op);
    pthread_join(tid, &status);  // pthread_join会阻塞主线程直至tid线程执行结束，此处使用是为了实现超时重传
}

void Master::waitMap(string filename){
    m_assign_lock.lock();
    runningMapWork.push_back(string(filename));  //将分配出去的map任务加入正在运行的工作队列
    m_assign_lock.unlock();
    pthread_t tid;
    pthread_create(&tid, NULL, waitMapTask, this); //创建一个用于回收计时线程及处理超时逻辑的线程
    pthread_detach(tid);
}

// map的worker只需要拿到对应的文件名就可以进行map
string Master::assignTask() {
    if(isMapDone()) return "empty";
    if(!m_list.empty()){
        m_assign_lock.lock();
        char* task = m_list.back(); // 从工作队列取出一个待map的文件名
        m_list.pop_back();            
        m_assign_lock.unlock();
        // waitMap(string(task));      // 调用waitMap将取出的任务加入正在运行的map任务队列并等待计时线程
        return string(task);
    }
    //m_hashSet.erase(task);
    return "empty";
}

// map任务完成
void Master::setMapStat(string taskTmp) {
    m_assign_lock.lock();
    finishedMapTask.emplace(taskTmp, 1);
    m_assign_lock.unlock();
}

// 检测map任务是否已全部完成
bool Master::isMapDone() {
    m_assign_lock.lock();
    if(finishedMapTask.size() != fileNum){  //当统计map任务的hashmap大小达到文件数，map任务结束
        m_assign_lock.unlock();
        return false;
    }
    m_assign_lock.unlock();
    return true;
}


int main(int argc, char* argv[]) {
    if (argc < 2) {
        cout << "Missing args! Format should be ./Master pg*.txt" << endl;
        exit(-1);
    }
    buttonrpc server;
    server.as_server(5555);
    Master master(2, 2);    // 优先级似乎在析构函数之上
    
    master.GetAllFile(argv, argc);
    server.bind("isMapDone", &Master::isMapDone, &master);
    server.bind("assignTask", &Master::assignTask, &master);
    server.bind("setMapStat", &Master::setMapStat, &master);
    server.bind("getMapNum", &Master::getMapNum, &master);
    server.bind("getReduceNum", &Master::getReduceNum, &master);
    server.run();
    return 0;
}