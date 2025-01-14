#include "nvm_btree.h"

NVMBtree::NVMBtree() {
    // bpnode *root = NewBpNode();
    // btree tmpbtree = btree(root);
}

void NVMBtree::Init(string &path, bool has_value) {
    if (has_value) {
        AllocatorInit(LOGPATH, NVM_LOG_SIZE, VALUEPATH, NVM_VALUE_SIZE, NODEPATH, NVM_NODE_SIZE);
    } else {
        AllocatorInit(LOGPATH, NVM_LOG_SIZE, NODEPATH, NVM_NODE_SIZE);
    }
    mybt = MyBtree::getInitial(path.c_str());
    bt = mybt->getBt();
    if(!bt) {
        assert(0);
    }
    // bpnode *root = NewBpNode();
    // btree tmpbtree = btree(root);
}

NVMBtree::~NVMBtree() {
    if(bt) {
        mybt->exitBtree();
        AllocatorExit();
    }
}
    
void NVMBtree::Insert(const unsigned long key, const string &value) {
    if(bt) {
        char *pvalue = value_alloc[my_thread_id % NUMACOUNT]->Allocate(value.size());
        nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);
        bt->btreeInsert(key, pvalue);
    }
}

void NVMBtree::Update(const unsigned long key, const string &value) {
    if(bt) {
        char *pvalue = value_alloc[my_thread_id % NUMACOUNT]->Allocate(value.size());
        nvm_memcpy_persist(pvalue, value.c_str(), value.size(), false);
        bt->btreeUpdate(key, pvalue);
    }
}

void NVMBtree::Insert(const unsigned long key, char *pvalue) {
    if(bt) {
        bt->btreeInsert(key, pvalue);
        /*
        char *value = bt->btree_search(key);

        if((unsigned long)value != key) {
            printf("Not Found Get key %llx, pvalue %llx\n", key, pvalue);
        } 

        if(key == 0x509af66da8d1399dUL) {
            printf("Insert key %llx", key);
        }
        */
    }
}

void NVMBtree::Delete(const unsigned long key) {
    if(bt) {
        bt->btreeDelete(key);
    }
}

int NVMBtree::Get(const unsigned long key, string &value) {
    char *pvalue = NULL;
    if(bt) {
        pvalue = bt->btreeSearch(key);
    }
    if(pvalue) {
        // print_log(LV_DEBUG, "Get pvalue is %p.", pvalue);
        value = string(pvalue, NVM_ValueSize);
        return 0;
    }
    return 2;
}

int NVMBtree::Get(const unsigned long key, char *&pvalue) {
    if(bt) {
        pvalue = bt->btreeSearch(key);
        //printf("Get key %llx, pvalue %llx\n", key, pvalue);
    }
    if(pvalue) {
        return 0;
    }
    return 1;

}

void NVMBtree::SeqRead(std::vector<std::string> &values, int &size) {
    bt->seq_read(values, size);
    cout<<"seq_read keys: " <<size<<endl;
}

void NVMBtree::GetRange(unsigned long key1, unsigned long key2, std::vector<std::string> &values, int &size) {
    if(bt) {
        bt->btreeSearchRange(key1, key2, values, size);
    }
}

void NVMBtree::GetRange(unsigned long key1, unsigned long key2, void **pvalues, int &size) {
    if(bt) {
        bt->btreeSearchRange(key1, key2, pvalues, size);
    }
}

void NVMBtree::Print() {
    if(bt) {
        bt->printAll();
    }
}

void NVMBtree::PrintInfo() {
    if(bt) {
        bt->PrintInfo();
        //show_persist_data();
    }
}

void NVMBtree::FunctionTest(int ops) {

}

void NVMBtree::motivationtest() {
    
}