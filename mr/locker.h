#ifndef LOCKER_H  // 防止头文件命名重复
#define LOCKER_H

#include <pthread.h>
#include <semaphore.h>
#include <iostream>

class locker{
public:
    locker(){  // 构造函数，直接在public中的写法
        if(pthread_mutex_init(&m_mutex, NULL) != 0){
            throw std::exception();
        }
    }
    ~locker(){  // 析构函数，在类对象被删除时调用
        pthread_mutex_destroy(&m_mutex);
    }
    bool lock(){
        return pthread_mutex_lock(&m_mutex) == 0;
    }
    bool unlock(){
        return pthread_mutex_unlock(&m_mutex) == 0;
    }
    pthread_mutex_t* getLock(){
        return &m_mutex;
    }
private:
    pthread_mutex_t m_mutex;
};

class sem{
public:
    sem(){
        if(sem_init(&m_sem, 0 ,0) != 0){
            throw std::exception();
        }
    }
    sem(int num){
        if(sem_init(&m_sem, 0, num) != 0){
            throw std::exception();
        }
    }
    ~sem(){
        sem_destroy(&m_sem);
    }
    bool wait(){
        return sem_wait(&m_sem) == 0;
    }
    bool post(){
        return sem_post(&m_sem) == 0;
    }
private:
    sem_t m_sem;
};

#endif