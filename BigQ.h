#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include<vector>
#include "File.h"
#include "Record.h"

using namespace std;

void *processBigQ(void*);
int GetRecSize (Record* r);

class BigQ {

        friend void *processBigQ(void *);
        friend bool compareRecordsFunc(Record * left, Record * right);


        public:
                BigQ (Pipe &inputPipe, Pipe &outputPipe, OrderMaker &sortOrder, int runLength);
                ~BigQ();

        private:
                Pipe& inputPipe;
                Pipe& outputPipe;
                OrderMaker& sortOrder;
                int runLength;

                static char *tempDirForRun;

                vector<int> runPagePos;
                int totalPagesWritten;

                File file;
                //char myFile[20];
                char *myFile;

                pthread_t workerThread;
};

#endif
