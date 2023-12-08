/**
 * Copyright (c) 2020 University of Luxembourg. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are
 * permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list
 * of conditions and the following disclaimer in the documentation and/or other materials
 * provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors may be
 * used to endorse or promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY OF LUXEMBOURG AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE UNIVERSITY OF LUXEMBOURG OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

/*
 * SliceFullAggregator.cpp
 *
 *  Created on:
 *      Author: vinu.venugopal
 */

#include "SliceFullAggregator.hpp"

#include <iostream>
#include <vector>
#include <cstring>
#include <iterator>
#include <string>
#include <sstream>
#include <unordered_map>
#include <mpi.h>
#include <ctime>

#include "../communication/Message.hpp"
#include "../serialization/Serialization.hpp"
#include "../slicing/Windows.hpp"

using namespace std;

SliceFullAggregator::SliceFullAggregator(int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {

	D(cout << "EVENTSHARDER [" << tag << "] CREATED @ " << rank << endl;)
	THROUGHPUT_LOG(datafile.open("Data/tp_log" + to_string(rank) + ".tsv")
	;
)
}

SliceFullAggregator::~SliceFullAggregator() {
D(cout << "EVENTSHARDER [" << tag << "] DELETED @ " << rank << endl;)
}

void SliceFullAggregator::batchProcess() {
D(cout << "EVENTSHARDER->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

void SliceFullAggregator::streamProcess(int channel) {

	D(cout << "EVENTFILTER->STREAMPROCESS [" << tag << "] @ " << rank
			<< " IN-CHANNEL " << channel << endl;)

	Message* inMessage, *outMessage;
	list<Message*>* tmpMessages = new list<Message*>();
	Serialization sede;

	//WrapperUnit wrapper_unit;
	EventDG eventDG;
	Slice slice;

	int c = 0;
	while (ALIVE) {

		pthread_mutex_lock(&listenerMutexes[channel]);

		while (inMessages[channel].empty())
			pthread_cond_wait(&listenerCondVars[channel],
					&listenerMutexes[channel]);

//		if(inMessages[channel].size()>1)
//				  cout<<tag<<" CHANNEL-"<<channel<<" BUFFER SIZE:"<<inMessages[channel].size()<<endl;
//

		while (!inMessages[channel].empty()) {
			inMessage = inMessages[channel].front();
			inMessages[channel].pop_front();
			tmpMessages->push_back(inMessage);
		}

		pthread_mutex_unlock(&listenerMutexes[channel]);

		while (!tmpMessages->empty()) {

			inMessage = tmpMessages->front();
			tmpMessages->pop_front();

			D(cout << "EVENTFILTER->POP MESSAGE: TAG [" << tag << "] @ " << rank
					<< " CHANNEL " << channel << " BUFFER " << inMessage->size
					<< endl;)

			sede.unwrap(inMessage);
			//if (inMessage->wrapper_length > 0) {
			//	sede.unwrapFirstWU(inMessage, &wrapper_unit);
			//	sede.printWrapper(&wrapper_unit);
			//}

			int offset = sizeof(int)
					+ (inMessage->wrapper_length * sizeof(WrapperUnit));
			Slice s;
			memcpy(&s, inMessage->buffer + offset, sizeof(Slice));
			// cout << "FOR s : " << rank << " " << s.id << " " << s.start_time << " " << s.end_time << " " << s.agg << endl;
			slice_aggregates[s.id] += s.agg;
			PARTIAL_AGG++;
			// cout << s.id << " " << s.agg << endl;
			slice_completed[s.id]++;


			

			if (slice_completed[s.id] == worldSize){
				memcpy(&slice, &s, sizeof(Slice));
				slice.agg = slice_aggregates[s.id];
				// cout << rank << " " << slice.id << " " << slice.start_time << " " << slice.end_time << " " << slice.agg << endl;
				outMessage = new Message(sizeof(Slice), inMessage->wrapper_length); // create new message with max. required capacity
				memcpy(outMessage->buffer, inMessage->buffer, offset);
				outMessage->size += offset;
				memcpy(outMessage->buffer + offset, &slice, sizeof(Slice));
				outMessage->size += sizeof(Slice);

			// Replicate data to all subsequent vertices, do not actually reshard the data here
			int n = 0;
			for (vector<Vertex*>::iterator v = next.begin(); v != next.end();
					++v) {
						int idx = n * worldSize + 0; 

				if (PIPELINE) {

					// Pipeline mode: immediately copy message into next operator's queue
					pthread_mutex_lock(&(*v)->listenerMutexes[idx]);
					(*v)->inMessages[idx].push_back(outMessage);

					D(cout << "EVENTFILTER->PIPELINE MESSAGE [" << tag << "] #"
							<< c << " @ " << rank << " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessage->size << " CAP "
							<< outMessage->capacity << endl;)

					pthread_cond_signal(&(*v)->listenerCondVars[idx]);
					pthread_mutex_unlock(&(*v)->listenerMutexes[idx]);

				} else {

					// Normal mode: synchronize on outgoing message channel & send message
					pthread_mutex_lock(&senderMutexes[idx]);
					outMessages[idx].push_back(outMessage);

					D(cout << "EVENTFILTER->PUSHBACK MESSAGE [" << tag << "] #"
							<< c << " @ " << rank << " IN-CHANNEL " << channel
							<< " OUT-CHANNEL " << idx << " SIZE "
							<< outMessage->size << " CAP "
							<< outMessage->capacity << endl;)

					pthread_cond_signal(&senderCondVars[idx]);
					pthread_mutex_unlock(&senderMutexes[idx]);
				}
				n++;
				
			}

			
			}
			delete inMessage;
			c++;
		}

		tmpMessages->clear();
	}

	delete tmpMessages;
}
