#include "RelOp.h"
#include "BigQ.h"
#include<algorithm>
#include<sstream>
#include<iostream>
using namespace std;

void *processSelectFile(void * args){
	cout<<__FUNCTION__<<endl;
	arguments *gotArgs = (arguments *) args;
	DBFile &inFile = *(gotArgs->inFile);
	Pipe &outPipe = *(gotArgs->outPipe);
	CNF &selOp = *(gotArgs->selOp);
	Record &literal = *(gotArgs->literal);
	Record fetchme;
	inFile.MoveFirst();

    int cnt = 0 ;
	while(inFile.GetNext(fetchme,selOp,literal)){
        ++cnt;
		outPipe.Insert(&fetchme);
	}
	cout<<__FUNCTION__<<"cnt: "<<cnt<<endl;

	outPipe.ShutDown();
}

void *processSelectPipe(void * args){
	cout<<__FUNCTION__<<endl;
	arguments *gotArgs = (arguments *) args;
	Pipe &inPipe = *(gotArgs->inPipe);
	Pipe &outPipe = *(gotArgs->outPipe);
	CNF &selOp = *(gotArgs->selOp);
	Record &literal = *(gotArgs->literal);
	Record fetchme;
	ComparisonEngine comp;

	while(inPipe.Remove(&fetchme)){
		if(comp.Compare (&fetchme, &literal, &selOp) != 0){
			outPipe.Insert(&fetchme);
		}
	}
	outPipe.ShutDown();
}
void *processSelectProject(void * args){
	cout<<__FUNCTION__<<endl;
	arguments *gotArgs = (arguments *) args;
	Pipe &inPipe = *(gotArgs->inPipe);
	Pipe &outPipe = *(gotArgs->outPipe);
	int *keepMe = gotArgs->keepMe;
	int &numAttsInput = gotArgs->numAttsInput;
	int &numAttsOutput = gotArgs->numAttsOutput;
	sort(keepMe,keepMe+numAttsOutput);
	Record fetchme;
 	Schema s("catalog","partsupp");	
	int cnt = 0;
	while(inPipe.Remove(&fetchme)){
		fetchme.Project(keepMe,numAttsOutput,numAttsInput);
		//fetchme.Print(&s);
		outPipe.Insert(&fetchme);
		cout<<"cnt: "<<cnt<<endl;
		++cnt;
	}
	cout<<"count in project: " <<cnt<<endl;;
	//cout<<"going out of processSelectProject"<<endl;
	outPipe.ShutDown();
}
void *processSum(void *args){
	cout<<__FUNCTION__<<endl;
	arguments *gotArgs = (arguments *) args;
	Pipe &inPipe = *(gotArgs->inPipe);
	Pipe &outPipe = *(gotArgs->outPipe);
	Function &computeMe = *(gotArgs->computeMe);
	Record fetchme;
	int resInt=0,resultInt = 0 ;
	double resultDouble = 0.0,resDouble = 0.0;
	Type type;	
	int cnt = 0 ;
	while(inPipe.Remove(&fetchme)){
		++cnt;
		type = computeMe.Apply(fetchme,resInt,resDouble);
		if(type == Int)
			resultInt += resInt;
		//if(type == Double)
		else
			resultDouble += resDouble;
	}
	stringstream ss;
	//clearing stringstream content
	ss.str("");
	Attribute A;
	if(type == Int){
		ss<<resultInt;
		A = {"int",Int};
	}
	else{
		ss<<resultDouble;
		A = {"double", Double};
	}
	string src = ss.str();
	src += "|";

	Schema out_sch("out_sch", 1, &A);
	Record sumRec;
	sumRec.ComposeRecord(&out_sch, src.c_str());
	outPipe.Insert(&sumRec);
    //sumRec.Print(&out_sch);
	outPipe.ShutDown();
}

void *processWriteOut(void *args){
	arguments *gotArgs = (arguments *) args;
	Pipe &inPipe = *(gotArgs->inPipe);
	Schema &mySchema = *(gotArgs->mySchema);
	FILE *outFile = (gotArgs->outFile);
	Record fetchme;
	while(inPipe.Remove(&fetchme)){
		fetchme.PrintToFile(&mySchema,outFile);	
	}
	fclose(outFile);
	//inPipe.ShutDown();
}

void *processDuplicateRemoval(void *args){
	arguments *gotArgs = (arguments *) args;
	Pipe &inPipe = *(gotArgs->inPipe);
	Pipe &outPipe = *(gotArgs->outPipe);
	Schema &mySchema = *(gotArgs->mySchema);
	Record fetchme;
	int pipesz = 500; // buffer sz allowed for each pipe

	Pipe tempOutPipe(pipesz);
	OrderMaker sortOrder(&mySchema);
	int runLength = 1; //hardcoding for now.
	BigQ bigQ(inPipe, tempOutPipe, sortOrder, runLength);
	Record left, right;
	tempOutPipe.Remove(&left);//get 1st record
	ComparisonEngine cEng;
	while(tempOutPipe.Remove(&right)){
		if(cEng.Compare(&left, &right, &sortOrder) == 0){
				//continue;
		}
		else{
			outPipe.Insert(&left);
			left.Copy(&right);
		}
	}
	outPipe.Insert(&left);
	tempOutPipe.ShutDown();
	outPipe.ShutDown();
}

void *processJoin(void *args){
	cout<<__FUNCTION__<<endl;
	arguments *gotArgs = (arguments *) args;
	Pipe &inPipeL = *(gotArgs->inPipeL);
	Pipe &inPipeR = *(gotArgs->inPipeR);
	Pipe &outPipe = *(gotArgs->outPipe);
	CNF &selOp = *(gotArgs->selOp);
	Record &literal = *(gotArgs->literal);
	Record fetchme1, fetchme2, fetchme;

	OrderMaker left, right;
	int buffsz = 100; // pipe cache size
	int runLength = 10;
	Pipe tempOutPipe1(buffsz), tempOutPipe2(buffsz);
 	//Schema s1("catalog","supplier");	
	//Schema s2("catalog","partsupp");	

	//if there is an acceptable ordering of given comparison
	if(selOp.GetSortOrders(left, right) != 0){
		BigQ bigQ1(inPipeL, tempOutPipe1, left, runLength);
		BigQ bigQ2(inPipeR, tempOutPipe2, right, runLength);
		int numAttsLeft = 7;//TODO: hardcoded for now
		int numAttsRight = 5;//TODO: hardcoded for now
		int numAttsToKeep = numAttsLeft+numAttsRight;

		int attsToKeep[numAttsToKeep];
		for(int i = 0;i<numAttsLeft;i++)
			attsToKeep[i] = i;
		for(int i = 0;i<numAttsRight;i++)
			attsToKeep[numAttsLeft+i] = i;
		int startOfRight = numAttsLeft;
		int status1 = 1 , status2 = 1;
		ComparisonEngine cEng;
		int cnt = 0 ;
		status1 = tempOutPipe1.Remove(&fetchme1);
		status2 = tempOutPipe2.Remove(&fetchme2);

		while(1){
			if(status1 == 0 || status2 == 0)
				break;
			int cmpStatus = cEng.Compare(&fetchme1, &left, &fetchme2, &right);
			if(cmpStatus == 0){ //join attributes are equal
				fetchme.MergeRecords (&fetchme1, &fetchme2, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, startOfRight);//this consumes right re
				outPipe.Insert(&fetchme);
				status2 = tempOutPipe2.Remove(&fetchme2);//handling only for q4 for now: assume left table has unique ids+ right table can have duplicates + TODO: HANDLE DUPLICATION IN BOTH TABLES!!!!!!!!!!
				++cnt;
			}
			else if(cmpStatus < 0)
				status1 = tempOutPipe1.Remove(&fetchme1);
			else
				status2 = tempOutPipe2.Remove(&fetchme2);
		}
		cout<<endl<<__FUNCTION__<<"cnt: " <<cnt<<endl;
	}
	//TODO:there is no acceptable ordering for the comparison: do the block nested loop join
	else {   }

	outPipe.ShutDown();
}

void *processGroupBy(void * args){
	cout<<__FUNCTION__<<endl;
	arguments *gotArgs = (arguments *) args;
	Pipe &inPipe = *(gotArgs->inPipe);
	Pipe &outPipe = *(gotArgs->outPipe);
	Function &computeMe = *(gotArgs->computeMe);
	OrderMaker &groupAtts = *(gotArgs->groupAtts);
	cout<<"ordermaker : ";
	groupAtts.Print();

	//remove me :starts here
	Attribute IA = {"int", Int};
	Attribute SA = {"string", String};
	Attribute DA = {"double", Double};

	Attribute s_nationkey = {"s_nationkey", Int};
	Attribute ps_supplycost = {"ps_supplycost", Double};
	Attribute joinatt[] = {IA,SA,SA,s_nationkey,SA,DA,SA,IA,IA,IA,ps_supplycost,SA};
	Schema join_sch ("join_sch", 12, joinatt);
	//ends here
	int runLength = 100;
	Record fetchme;
	int buffsz = 100000; // pipe cache size
	Pipe tempOutPipe(buffsz);
	BigQ bigQ1(inPipe, tempOutPipe, groupAtts, runLength);
	ComparisonEngine cEng;

	Record left, right;
	tempOutPipe.Remove(&left);//get 1st record

	Sum s;
	Pipe SumInPipe(buffsz);
	s.Run(SumInPipe, outPipe, computeMe);

	while(tempOutPipe.Remove(&right)){
		// Records are unequal
		if(cEng.Compare(&left, &right, &groupAtts) != 0){
			SumInPipe.Insert(&left);
			SumInPipe.ShutDown();
			s.WaitUntilDone();
			s.Run(SumInPipe, outPipe, computeMe);
		}
		// EQUAL
		else{
			SumInPipe.Insert(&left);
		}
		//left.Copy(&right);
		left.Consume(&right);
	}
	cout<<"out"<<endl;
	SumInPipe.Insert(&left);
	SumInPipe.ShutDown();
	s.WaitUntilDone();
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	args = new arguments;
	args->inFile = &inFile;
	args->outPipe = &outPipe;
	//args->inPipe = NULL;//not needed for this class
	args->selOp = &selOp;
	args->literal = &literal;
	int threadCreateStatus = pthread_create(&selectFileThread,NULL,processSelectFile,(void *)args);
}

void SelectFile::WaitUntilDone () {
	pthread_join (selectFileThread, NULL);
	delete args;
}

void SelectFile::Use_n_Pages (int runlen) {

}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { 
	args = new arguments;
	args->inPipe = &inPipe;    
	args->selOp = &selOp;
	args->outPipe = &outPipe;
	args->literal = &literal;
	int threadCreateStatus = pthread_create(&selectFileThread,NULL,processSelectPipe,(void *)args);

}

void SelectPipe::WaitUntilDone () {
	pthread_join (selectFileThread, NULL);
	delete args;
}

void SelectPipe::Use_n_Pages (int runlen) {

}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int * keepMe, int numAttsInput, int numAttsOutput){
	args = new arguments;
	args->inPipe = &inPipe;    
	args->outPipe = &outPipe;
	args->keepMe = keepMe;
	args->numAttsInput = numAttsInput;
	args->numAttsOutput = numAttsOutput;
	cout<<"run size: "<<numAttsOutput<<endl;
	int threadCreateStatus = pthread_create(&selectFileThread,NULL,processSelectProject,(void *)args);

}

void Project::WaitUntilDone (){
	pthread_join (selectFileThread, NULL);
	delete args;
}
void Project::Use_n_Pages (int n){

}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
	args = new arguments;
	args->inPipe = &inPipe;    
	args->outPipe = &outPipe;
	args->computeMe = &computeMe;
	int threadCreateStatus = pthread_create(&selectFileThread,NULL,processSum,(void *)args);
}

void Sum::WaitUntilDone (){
	pthread_join (selectFileThread, NULL);
	delete args;
}

void Sum::Use_n_Pages (int n){
}

void DuplicateRemoval:: Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
	args = new arguments;
	args->inPipe = &inPipe; 
	args->outPipe = &outPipe;
	args->mySchema = &mySchema;
	int threadCreateStatus = pthread_create(&selectFileThread,NULL,processDuplicateRemoval,(void *)args);
}

void DuplicateRemoval::WaitUntilDone () {
	pthread_join (selectFileThread, NULL);
	delete args;
}

void DuplicateRemoval::Use_n_Pages (int n) { 
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema){
	args = new arguments;
	args->inPipe = &inPipe; 
	args->outFile = outFile;
	args->mySchema = &mySchema;
	int threadCreateStatus = pthread_create(&selectFileThread,NULL,processWriteOut,(void *)args);

}
void WriteOut::WaitUntilDone (){
	pthread_join (selectFileThread, NULL);
	delete args;
}
void WriteOut::Use_n_Pages (int n){
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
	args = new arguments;
	args->inPipeL = &inPipeL; 
	args->inPipeR = &inPipeR; 
	args->outPipe = &outPipe;
	args->selOp = &selOp;
	args->literal = &literal;
	int threadCreateStatus = pthread_create(&selectFileThread,NULL,processJoin,(void *)args);
}
void Join::WaitUntilDone () { 
	pthread_join (selectFileThread, NULL);
	delete args;
}
void Join::Use_n_Pages (int n) { 
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
	args = new arguments;
	args->inPipe = &inPipe;    
	args->outPipe = &outPipe;
	args->groupAtts = &groupAtts;
	args->computeMe = &computeMe;
	int threadCreateStatus = pthread_create(&selectFileThread,NULL,processGroupBy,(void *)args);
}

void GroupBy::WaitUntilDone (){
	pthread_join (selectFileThread, NULL);
	delete args;
}

void GroupBy::Use_n_Pages (int n){
}
