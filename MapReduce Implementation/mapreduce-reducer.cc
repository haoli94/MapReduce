/**
 * File: mapreduce-reducer.cc
 * --------------------------
 * Presents the implementation of the MapReduceReducer class,
 * which is charged with the responsibility of collating all of the
 * intermediate files for a given hash number, sorting that collation,
 * grouping the sorted collation by key, and then pressing that result
 * through the reducer executable.
 *
 * See the documentation in mapreduce-reducer.h for more information.
 */

#include "mapreduce-reducer.h"
#include "ostreamlock.h" 
#include "mr-names.h"
#include <iostream>
using namespace std;

MapReduceReducer::MapReduceReducer(const string& serverHost, unsigned short serverPort,
		const string& cwd, const string& executable, const string& outputPath) : 
	MapReduceWorker(serverHost, serverPort, cwd, executable, outputPath){}
	
void MapReduceReducer::reduce() const {

	while (true) {
		string name;
                if (!requestInput(name)) break;
                alertServerOfProgress("About to process \"" + name + "\".");
		string base = extractBase(name);
                string command = "cat " + name;
		string script = cwd + "/group-by-key.py";
                command += " | sort | python " + script + " > ";
		string intermidiatePath = outputPath + "/" + changeExtension(base, "mapped", "mid");
		command += intermidiatePath;
                system(command.c_str());
                string after = outputPath + "/" + changeExtension(base, "mapped", "output");
                bool success = processInput(intermidiatePath, after);
                remove(intermidiatePath.c_str());
		//https://stackoverflow.com/questions/8176929/removing-files-from-c-code
		//refer to how to remove files in C language
                notifyServer(name, success);

	}

	alertServerOfProgress("Server says no more input chunks, so shutting down.");



}
