#include <iostream>
#include <string>
#include <vector>
#include <ctime>
#include <cstdlib>
#include <sys/time.h>
#include <iomanip>

#include "index/NUMA_Config.h"

using namespace std;

#define NUM (64 * 1024 * 1024)
#define SIZE 8

pmem::obj::pool_base pool;
char * buf[SIZE];
double *data_array;
int random_array[NUM];

void init_data(double *data_, int n){
    int i;
    for (i = 0; i < n; i++) { 
        data_[i] = i;
        pmemobj_persist(pool.handle(), &data_[i], sizeof(double));
    }
} 
 
void seque_read(int elems, int stride) { //数组大小，步长
    int i; 
    double result = 0.0; 
    volatile double sink; 
    
    for (i = 0; i < elems; i += stride) { 
        double * addr = &data_array[i];
        memcpy(buf, addr, SIZE);
    } 
    sink = result; 
}

void random_read(int* random_index_arr, int count) { //随机数数组起始地址，数据数组大小
    int i;
    double result = 0.0; 
    volatile double sink; 
    
    for (i = 0; i < count; i++) {
        double * addr = &data_array[*(random_index_arr+i)];
        memcpy(buf, addr, SIZE);
    }
    sink = result; 
}

void seque_write(int elems, int stride) { //数组大小，步长
    int i;
    
    for (i = 0; i < elems; i += stride) { 
        double * addr = &data_array[i];
        memcpy(addr, buf, SIZE);
        pmemobj_persist(pool.handle(), addr, SIZE);
    } 
}

void random_write(int* random_index_arr, int count) { //随机数数组起始地址，数据数组大小
    int i;
    
    for (i = 0; i < count; i++) {
        double * addr = &data_array[*(random_index_arr+i)];
        memcpy(addr, buf, SIZE);
        pmemobj_persist(pool.handle(), addr, SIZE);
    }
}

void random_access_adjust(int* random_index_arr, int count) { //随机数数组起始地址，数据数组大小
    int i;
    double result = 0.0;
    
    for (i = 0; i < count; i++) {
        result += *(random_index_arr+i);
    }
}

int main(void) {
    string pool_name("/mnt/pmem0/test");
    remove(pool_name.c_str());
    pool = pmem::obj::pool<int>::create(
        pool_name.c_str(), "WQ", (NUM + 4) * sizeof(double) * 2, S_IWUSR | S_IRUSR);
    PMEMoid oid;
    if (pmemobj_alloc(pool.handle(), &oid, (NUM + 4) * sizeof(double), 0, nullptr, nullptr))
    {
        fprintf(stderr, "fail to alloc nvm\n");
        exit(-1);
    }
    data_array = (double *)pmemobj_direct(oid);

    init_data(data_array, NUM);
    vector<int> stride = {1,32,64};
    vector<int> num = {32*1024, 64*1024, 256*1024, 512*1024, 2*1024*1024, 8*1024*1024, 16*1024*1024, 64*1024*1024};
    vector<vector<double>> time_seque(stride.size(), vector<double>(num.size(), 0));
    vector<double> time_random(num.size(), 0);

    struct timespec start, end, end2;

    srand((int)time(0));
    for (int i = 0; i < NUM; i++) {
        random_array[i] = rand() % NUM;
    }

    // read
    for (int i = 0; i < stride.size(); i++) {
        for (int j = 0; j < num.size(); j++) {
            clock_gettime(CLOCK_REALTIME, &start);
            seque_read(num[j], stride[i]);
            clock_gettime(CLOCK_REALTIME, &end);
            time_seque[i][j] = (end.tv_sec*1000000000 + end.tv_nsec) - (start.tv_sec*1000000000 + start.tv_nsec);
            time_seque[i][j] = time_seque[i][j] / (num[j]/stride[i]);
        }
    }

    for (int j = 0; j < num.size(); j++) {
        clock_gettime(CLOCK_REALTIME, &start);
        random_read(random_array, num[j]);
        clock_gettime(CLOCK_REALTIME, &end);
        random_access_adjust(random_array, num[j]);
        clock_gettime(CLOCK_REALTIME, &end2);
        time_random[j] = (end.tv_sec*1000000000 + end.tv_nsec) - (start.tv_sec*1000000000 + start.tv_nsec)
            - ((end2.tv_sec*1000000000 + end2.tv_nsec) - (end.tv_sec*1000000000 + end.tv_nsec));
        time_random[j] = time_random[j] / num[j];
    }

    cout << "[read]\nstride\t";
    for (int j = 0; j < num.size(); j++) {
        cout << num[j]/1024 << "\t";
    }
    cout << endl;
    for (int i = 0; i < stride.size(); i++) {
        cout << stride[i] << "\t";
        for (int j = 0; j < num.size(); j++) {
            cout << fixed << setprecision(3) << time_seque[i][j] << "\t";
        }
        cout << endl;
    }
    cout << "random" << "\t";
    for (int j = 0; j < num.size(); j++) {
        cout << fixed << setprecision(3) << time_random[j] << "\t";
    }
    cout << endl;

    // write
    for (int i = 0; i < stride.size(); i++) {
        for (int j = 0; j < num.size(); j++) {
            clock_gettime(CLOCK_REALTIME, &start);
            seque_write(num[j], stride[i]);
            clock_gettime(CLOCK_REALTIME, &end);
            time_seque[i][j] = (end.tv_sec*1000000000 + end.tv_nsec) - (start.tv_sec*1000000000 + start.tv_nsec);
            time_seque[i][j] = time_seque[i][j] / (num[j]/stride[i]);
        }
    }

    for (int j = 0; j < num.size(); j++) {
        clock_gettime(CLOCK_REALTIME, &start);
        random_write(random_array, num[j]);
        clock_gettime(CLOCK_REALTIME, &end);
        random_access_adjust(random_array, num[j]);
        clock_gettime(CLOCK_REALTIME, &end2);
        time_random[j] = (end.tv_sec*1000000000 + end.tv_nsec) - (start.tv_sec*1000000000 + start.tv_nsec)
            - ((end2.tv_sec*1000000000 + end2.tv_nsec) - (end.tv_sec*1000000000 + end.tv_nsec));
        time_random[j] = time_random[j] / num[j];
    }

    cout << "[write]\nstride\t";
    for (int j = 0; j < num.size(); j++) {
        cout << num[j]/1024 << "\t";
    }
    cout << endl;
    for (int i = 0; i < stride.size(); i++) {
        cout << stride[i] << "\t";
        for (int j = 0; j < num.size(); j++) {
            cout << fixed << setprecision(3) << time_seque[i][j] << "\t";
        }
        cout << endl;
    }
    cout << "random" << "\t";
    for (int j = 0; j < num.size(); j++) {
        cout << fixed << setprecision(3) << time_random[j] << "\t";
    }
    cout << endl;

    return 0;
}

