#include "BigQ.h"
#include<stdlib.h>
#include<vector>
#include<cmath>
#include<algorithm>
#include<queue>
#include<string>
#include<sstream>
using namespace std;
#define MAX_RUNS 100

//global variable to access within compareRecordsFunc 
OrderMaker *sortOrder;

class compareRecords{
    public:
        bool operator() (const Record * left,const Record * right) const {
            //returns a negative number, a 0, or a positive number if left is less than, equal to, or greater than right. 
            ComparisonEngine cmpEngine;
            int returnT = cmpEngine.Compare((Record *)left,(Record *) right, sortOrder);
            if(returnT < 0)
                return true;
            else
                return false;
        }
};

class recordComparisonForPriorityQueue{
    public:
    bool operator() (const pair<Record *, int> pair1, const pair<Record *, int> pair2) const {
        ComparisonEngine cmpEngine;
        int returnT = cmpEngine.Compare(pair1.first,pair2.first,sortOrder);
        if(returnT < 0)
            return false;
        else 
            return true;
    }
};

void printErrorAllocation(){
    cout<<endl<<"Out of memory !!!"<<endl ;
}

int GetRecSize (Record* r){
    char* b = r->GetBits();
    //if(b == NULL)
    //    return 0;//some bug 
    // This holds the size of the record
    int v = ((int *)b)[0];
    return v;
}

BigQ :: BigQ (Pipe &inputPipe, Pipe &outputPipe, OrderMaker &sortOrder, int runLength):
    inputPipe(inputPipe),outputPipe(outputPipe),sortOrder(sortOrder),runLength(runLength)
{
    int rc = pthread_create(&workerThread, NULL, processBigQ, (void *) this);
    if(rc)
    {
        printf("Error while creating a thread");
        exit(-1);
    }
}

void * processBigQ(void *arg)
{
    BigQ *obj = (BigQ *)arg;
    Pipe &in = obj->inputPipe;
    Pipe &out = obj->outputPipe;
    sortOrder = &(obj->sortOrder);
    int runLength = obj->runLength;

    //array to keep track of the number of records in each run:initialize each to 0
    int numRecordsInEachRun[MAX_RUNS] = {0};

    int runNumber = 0;

    //Schema *s = new Schema("catalog", "customer");
    Schema *s = new Schema("catalog", "lineitem");

    vector<Record *> recordVector;
    bool phaseOneOver = false;
    //keeping track of last record which could not get added to vector because of exceeding page size
    Record *lastRecOfPage = NULL;
    //calculate size of runLength number of pages: you should ideally check for overflows!!!
    unsigned int runLenSize = PAGE_SIZE * runLength;
    vector<pair<int,int> >endPageNumOfRun; //stores start and end page numbers

    Record * temp = NULL;
    //Record temp;
    Page page;
    unsigned int pageCounter = 0 ;
    File file;
    //change the name of this file : hardcoding for now
    srand(time(NULL));
    stringstream ss;
    ss<< rand();
    string fileNameStr = "outputBigQ_" + ss.str() + ".bin";
    char * ff = (char *)fileNameStr.c_str(); 
    cout<<"fname: "<<ff<<endl;
    
    file.Open(0,ff);

    int totalRecordCount = 0;
    int idxRun = 0;
    unsigned int curSize = 0;
    
    while(1){
        
        //get all records for runLength number of pages
        while(1){
            temp = new(std::nothrow) Record;
            if(temp == NULL){
                cout<<"No memory for allocating new record"<<endl;
                exit(-1);
            }
            if (in.Remove(temp)) {
                curSize += GetRecSize(temp);
                if(curSize > runLenSize) {
                    break;
                }
                recordVector.push_back(temp);
                continue;
            }
            else {
                //pipe is empty now 
                phaseOneOver = true;
                temp = NULL;
                break;
            }
        }

        totalRecordCount += recordVector.size();

        //sort vector 
        sort(recordVector.begin(),recordVector.end(),compareRecords());
        Record *l, *r;
        ComparisonEngine cEng;
        for (int cc = 1; cc < recordVector.size(); cc++) {
            l = recordVector[cc-1];
            r = recordVector[cc];

            if (cEng.Compare(l,r,sortOrder) > 0) {
                cout << "******* NOT SORTED **********" << endl;
                l->Print(s);
                r->Print(s);
            }
        }

        int startPageNum = pageCounter;
        //put sorted vector into runLength number of Pages 
        for(int i =0 ; i<recordVector.size(); i++){
            //temporarily save the record as we may lose the record while appending to a full page: no need as adding to
            //full page does not consume the record
            if(!page.Append(recordVector[i])){//NOTE:append function consumes the record
                //page full: write to file
                file.AddPage(&page,pageCounter);
                page.EmptyItOut();
                //add the last record which couldnt get added
                page.Append(recordVector[i]);
                ++pageCounter;
            }
        }
        file.AddPage(&page,pageCounter);//write the last page to file: it didnt get full
        endPageNumOfRun.push_back(make_pair(startPageNum,pageCounter));
        ++pageCounter;
        page.EmptyItOut();
        recordVector.clear();
        curSize = 0;

        //phase 1 over:
        if(phaseOneOver == true){
            break;
        }
        
        //keep track of the lost record
        recordVector.push_back(temp);
        curSize += GetRecSize(temp);
    }

    file.Close();
    
    //PHASE 2 STARTS!!
    //construct priority queue over sorted runs and dump sorted data into the out pipe
    priority_queue<pair<Record *,int>,vector<pair<Record *,int> >, recordComparisonForPriorityQueue >pqRecords;
    
    //open file and get the right page numbers in memory
    File fileNew;
    fileNew.Open(1,"outputBigQ.bin");
    int numPagesInFile = fileNew.GetLength();
    //total number of runs
    int totalRuns = endPageNumOfRun.size();
    int numRecordsInCurrentRun = 0 ;
    int currentPageNumOfRun[totalRuns];

    //the page number that is needed to be brought in memory for merging for each run index.
    Page pagesNeeded[totalRuns];
    //create records for each run to be allocated for priority queue
    Record tempRec[totalRuns];

    for(int runNumber=0;runNumber<totalRuns;runNumber++){
        int startPageNum = endPageNumOfRun[runNumber].first;
        fileNew.GetPage(&pagesNeeded[runNumber],startPageNum);
        pagesNeeded[runNumber].GetFirst(&tempRec[runNumber]);
        pqRecords.push(make_pair(&tempRec[runNumber],runNumber));
        currentPageNumOfRun[runNumber] = startPageNum;
    }

    int id =0;

    while(!pqRecords.empty()){
        id++;
        Record * record = pqRecords.top().first;
        int runIdxBelongsTo = pqRecords.top().second;
        pqRecords.pop();
        //insert record into out pipe now

        out.Insert(record);
        runNumber = runIdxBelongsTo;

        int status = pagesNeeded[runNumber].GetFirst(&tempRec[runNumber]);
        //success in getting next record from the page for the run
        if(status == 1){
            pqRecords.push(make_pair(&tempRec[runNumber],runNumber));
        }
        //else could not get next record from current page of the current run, then see if you can get record from the
        //next page of the current run
        else{
            
            int pageNumCurrentRun = currentPageNumOfRun[runIdxBelongsTo]; 
            if(pageNumCurrentRun >= numPagesInFile -1){
                cout<<"SOME BUG!!!!";
                continue;
            }

            ++pageNumCurrentRun;
            //update currentPage number of the Run to which the popped record belongs:
            if((pageNumCurrentRun >= numPagesInFile-1) || pageNumCurrentRun > endPageNumOfRun[runIdxBelongsTo].second) {
                continue;
            }
            currentPageNumOfRun[runNumber] = pageNumCurrentRun;

            //load the next page in memory for current Run 
            fileNew.GetPage(&pagesNeeded[runNumber],pageNumCurrentRun);
            //push record from next page
            pagesNeeded[runNumber].GetFirst(&tempRec[runNumber]);
            pqRecords.push(make_pair(&tempRec[runNumber],runNumber));

        }
    }

    fileNew.Close();

    out.ShutDown();
   
    return NULL;
}

BigQ::~BigQ () {
}


