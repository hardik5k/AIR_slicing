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
 * WindowAggregator.cpp
 *
 *  Created on: 11, Jul, 2019
 *      Author: vinu.venugopal
 */

#include "WindowAggregator.hpp"

#include <mpi.h>
#include <cstring>
#include <iostream>
#include <list>
#include <unistd.h>

#include "../communication/Message.hpp"
#include "../communication/Window.hpp"
#include "../serialization/Serialization.hpp"
#include "../slicing/Windows.hpp"
using namespace std;

WindowAggregator::WindowAggregator(int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {

	D(cout << "FULLAGGREGATOR [" << tag << "] CREATED @ " << rank << endl;);
}

WindowAggregator::~WindowAggregator() {
	D(cout << "FULLAGGREGATOR [" << tag << "] DELETED @ " << rank << endl;);
}

void WindowAggregator::batchProcess() {
	D(cout << "FULLAGGREGATOR->BATCHPROCESS [" << tag << "] @ " << rank << endl;);
}

void WindowAggregator::streamProcess(int channel) {

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
			Slice slice;
			memcpy(&slice, inMessage->buffer + offset, sizeof(Slice));
			for (auto it = CURRENT_WINDOWS.begin(); it != CURRENT_WINDOWS.end(); it++){

				int id = it->first;
				int r = it->second->get_r();
				int s = it->second->get_s();
				long int query_start = it->second->get_start();

				if (slice.end_time <= query_start) continue;
			
				long int slice_mid = slice.start_time + (slice.end_time - slice.start_time) / 2;
				int win_id = (slice_mid - query_start) / s;
				int mod = (slice_mid - query_start) % s;


				window_aggregates[id][win_id] += slice.agg;
				FULL_AGG++;
				window_completeness[id][win_id] += slice.end_time - slice.start_time;

				if (window_completeness[id][win_id] == r){
						long int tn = (long int)MPI_Wtime() * 1000 + 999;
						cout << "Query : " << id << " WIN_ID : " << win_id << " AGG : " << window_aggregates[id][win_id]<<  " LATENECY : " << tn << " " << query_start <<  endl;
 					}

			

				if (mod <= r - s){
					win_id--;
					window_aggregates[id][win_id] += slice.agg;
					FULL_AGG++;
					window_completeness[id][win_id] += slice.end_time - slice.start_time;


					if (window_completeness[id][win_id] == r){
						long int tn = (long int)MPI_Wtime() * 1000 + 999;
						cout << "Query : " << id << " WIN_ID : " << win_id << " AGG : " << window_aggregates[id][win_id] <<   " TOTAL_AGG : " << PARTIAL_AGG + FULL_AGG << endl;
 					}

				}

			}
			
		delete inMessage;
		c++;
		}

		tmpMessages->clear();
	}

	delete tmpMessages;
}
