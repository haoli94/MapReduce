/**
 * File: mapreduce-mapper.cc
 * -------------------------
 * Presents the implementation of the MapReduceMapper class,
 * which is charged with the responsibility of pressing through
 * a supplied input file through the provided executable and then
 * splaying that output into a large number of intermediate files
 * such that all keys that hash to the same value appear in the same
 * intermediate.
 */

#include "mapreduce-mapper.h"
#include "mr-names.h"
#include "string-utils.h"
#include <vector>
#include <fstream>
#include <iomanip>
#include <sstream>
using namespace std;

MapReduceMapper::MapReduceMapper(const string& serverHost, unsigned short serverPort,
		const string& cwd, const string& executable,
		const string& outputPath, unsigned short numHashCodes) :
	MapReduceWorker(serverHost, serverPort, cwd, executable, outputPath),hashCode(numHashCodes){}
	
void MapReduceMapper::map() const {
	while (true) {
		string name;
		if (!requestInput(name)) break;
		alertServerOfProgress("About to process \"" + name + "\".");
		string base = extractBase(name);
		string output = outputPath + "/" + changeExtension(base, "input", "mapped");
		bool success = processInput(name, output);
		string key;
		int value;
		ifstream inFile(output);
		vector<ofstream> outPutStreams;
		// https://stackoverflow.com/questions/28166794/initialise-vector-of-stdofstream-in-c
		// refer to vector of ofstream pointers
    		for(unsigned short i = 0; i<hashCode; i++){
			ofstream hashedOutFile;
			outPutStreams.push_back(move(hashedOutFile));
    		}
		while(inFile >> key >> value) {
			size_t hashValue = hash<string>()(key) % hashCode;
			string newS;
			stringstream trans;
			trans << setw(5) << setfill('0') << hashValue;
			trans >> newS;
			//https://stackoverflow.com/questions/48030065/c-stdstringstream-operations-optimizations
			//refer to the usage of setw and setfill
			newS += ".mapped";
			string out = outputPath + "/" + changeExtension(base, "input", newS);
			if(!outPutStreams[hashValue].is_open()) outPutStreams[hashValue].open(out, ios::app);
			outPutStreams[hashValue] << key << " " << value << endl;
		}
		inFile.close();
		for(size_t i=0; i < outPutStreams.size(); i++){
			outPutStreams[i].close();
		} 
		remove(output.c_str());
		notifyServer(name, success);
	}

	alertServerOfProgress("Server says no more input chunks, so shutting down.");
}
