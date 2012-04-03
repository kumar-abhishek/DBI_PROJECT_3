#include "TwoWayList.h"
#include<iostream>
#include<fstream>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
	currentRecord = new Record;
	currentPage = new Page;
	filePtr = new File;
	curPageNum = 0;
}

DBFile::~DBFile () {
    delete currentRecord;
    delete currentPage;
    delete filePtr;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
	filePtr->Open(0,f_path);
	//filePtr->Close();
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	//loadpath includes tpch_dir+schema name
	cout<<"loadpath: "<<loadpath<<endl;
	//open file for reading
	FILE * fp = fopen(loadpath,"r");
	int pageCounter = 0;
	while(currentRecord->SuckNextRecord(&f_schema,fp) == 1){
		// append to the currentPage.
		int status = currentPage->Append(currentRecord);
  
 		if(status == 0) {//page full: if the current page is full, make a new page else 
			filePtr->AddPage(currentPage,pageCounter);//write the page to file
			currentPage->EmptyItOut();
			currentPage->Append(currentRecord);//now add the previous record to the new page
			++pageCounter;
		}
	}
	filePtr->AddPage(currentPage,pageCounter);//write the last page to file: it didnt get full
	
	//set number of pages in DBFile
	numberOfPages = pageCounter;

	//remove the page content: not needed
//	currentPage->EmptyItOut();

	cout<<"No of pages: "<<pageCounter<<" |" <<filePtr->GetLength()<<endl;
}

int DBFile::Open (char *f_path) {
	//need to return 1 for success , 0 for failure.
	//f.Open will give error and exit in case it could not open the file

	//1st parameter: fileLen is 1 because you dont recreate it: just open it.
	filePtr->Open(1,f_path);
}

void DBFile::MoveFirst () {
	filePtr->GetPage(currentPage,0);
}

int DBFile::Close () {
	//need to return 1 for success , 0 for failure.
	filePtr->Close();
}

void DBFile::Add (Record &rec) {
	int lastPage = filePtr->GetLength();
	cout<<endl<<__FUNCTION__<<__FILE__<<"lastPage: "<<lastPage<<"| length: "<<filePtr->GetLength()<<endl;
	    
	if(!currentPage->Append(&rec)){
        filePtr->AddPage(currentPage,curPageNum);//write the page to file
        ++curPageNum;
        currentPage->EmptyItOut();
        currentPage->Append(&rec);

    }

	//need to see whether updating the last page automatically updates the file or not.
}

int DBFile::GetNext (Record &fetchme) {
	//check whether you are within page limit of file

	if(curPageNum + 1 >= filePtr->GetLength())
		return 0;

	if(currentPage->GetFirst(currentRecord) != 1) //if there is no record left on currentPage, read next page from file into currentPage
	{
		++curPageNum;
		if(curPageNum + 1 >= filePtr->GetLength())
			return 0;

		filePtr->GetPage(currentPage,curPageNum);
		currentPage->GetFirst(currentRecord);
	}

//	fetchme = *currentRecord;
//	doubt: is the currentPage variable in this class just storing the copy of the records or are they storing actual records
//	fixed this above disaster
	fetchme.Copy(currentRecord);
	return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	if(GetNext(fetchme) == 0)
		return 0;
	ComparisonEngine comp;
	while(comp.Compare (&fetchme, &literal, &cnf) != 1){
		if(GetNext(fetchme) == 0 )
			return 0;
	}
		return 1;
	
}
