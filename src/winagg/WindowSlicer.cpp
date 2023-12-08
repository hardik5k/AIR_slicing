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
 * WindowSlicer.cpp
 *
 *  Created on:
 *      Author: vinu.venugopal
 */

#include "WindowSlicer.hpp"

#include <iostream>
#include <vector>
#include <cstring>
#include <iterator>
#include <string>
#include <sstream>
#include <unordered_map>
#include <mpi.h>
#include <ctime>
#include <sys/ipc.h> 
#include <sys/msg.h> 

#include "../communication/Message.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

struct mesg_buffer { 
    long mesg_type; 
	int r, s;
} message_add;

struct mesg_buffer1 { 
    long mesg_type; 
	int id;
} message_remove;

WindowSlicer::WindowSlicer(int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {
	this->windows = CURRENT_WINDOWS;
	D(cout << "EVENTSHARDER [" << tag << "] CREATED @ " << rank << endl;)
	THROUGHPUT_LOG(datafile.open("Data/tp_log" + to_string(rank) + ".tsv")
	;
)
}

WindowSlicer::~WindowSlicer() {
D(cout << "EVENTSHARDER [" << tag << "] DELETED @ " << rank << endl;)
}

void WindowSlicer::batchProcess() {
D(cout << "EVENTSHARDER->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

void WindowSlicer::streamProcess(int channel) {

	long int ts = (long int)MPI_Wtime() * 1000 + 999;

	// key_t key; 
    // int msgid; 
  
    // // ftok to generate unique key 
    // key = ftok("progfile", 65); 
  
    // // msgget creates a message queue 
    // // and returns identifier 
    // msgid = msgget(key, 0777 | IPC_CREAT); 


	D(cout << "EVENTFILTER->STREAMPROCESS [" << tag << "] @ " << rank
			<< " IN-CHANNEL " << channel << endl;)

	Message* inMessage, *outMessage;
	list<Message*>* tmpMessages = new list<Message*>();
	Serialization sede;

	//WrapperUnit wrapper_unit;
	EventDG eventDG;
	EventFT eventFT;

	int c = 0;
	long int time_now = (long int)MPI_Wtime() * 1000 + 999;
	sm = new SliceManager(this->windows, time_now);
	int slice_id = 0;
	int residue_num = 1;
	int residue_den = 0;

	for (int i = 1; i <= 1; i++){
		CURRENT_WINDOWS[i] = new PairedWindow(1, 18000, 15000, time_now);
	}
	// CURRENT_WINDOWS[1] = new PairedWindow(1, 18000, 15000, time_now);
	// CURRENT_WINDOWS[2] = new PairedWindow(2, 12000, 10000, time_now);
	sm->add_edges(time_now, CURRENT_WINDOWS[1]);
	// sm->add_edges(time_now, CURRENT_WINDOWS[2]);

	long int next_edge = sm->get_next_edge();

	while (ALIVE) {

	// 	// msgrcv to receive message 
    // while (msgrcv(msgid, &message_add, sizeof(message_add), 1, IPC_NOWAIT) != -1) {
	// 	long int tn = (long int)MPI_Wtime() * 1000 + 999;
    //     CURRENT_WINDOWS[++NUM_QUERIES] = new PairedWindow(1, message_add.r, message_add.s, tn);
	// 	sm->add_edges(tn, CURRENT_WINDOWS[++NUM_QUERIES]);
		
    // }

	// while (msgrcv(msgid, &message_remove, sizeof(message_remove), 2, IPC_NOWAIT) != -1) {
	// 		CURRENT_WINDOWS.erase(message_remove.id);
	// 		sm->remove_edges(message_remove.id);
    // }

	// long int tn = (long int)MPI_Wtime() * 1000 + 999;
	// if (tn == ts + 1000){
		
	// 	CURRENT_WINDOWS[2] = new PairedWindow(2, 12000, 10000, sm->get_next_edge());
	// 	sm->add_edges(sm->get_next_edge(), CURRENT_WINDOWS[2]);
		
	// 	next_edge = sm->get_next_edge();
		
	// }

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

			outMessage = new Message(inMessage->size - offset,
					inMessage->wrapper_length); // create new message with max. required capacity

			WrapperUnit slice_wrapper_unit;
			memcpy(&slice_wrapper_unit, inMessage->buffer + 4, sizeof(WrapperUnit));
			
			slice_wrapper_unit.slice_id = slice_id;
			// cout << "NEXT_EDGE: " << next_edge << endl;
			// cout << slice_wrapper_unit.slice_id << " " << slice_wrapper_unit.window_start_time << endl;

			// when last message of the current slice.
			if (slice_wrapper_unit.window_start_time >= next_edge - 1000){
				slice_wrapper_unit.completeness_tag_denominator = residue_den;
				residue_den = 0;
				slice_id++;
				next_edge = sm->advance_window_get_next_edge();

			}
			else {
				slice_wrapper_unit.completeness_tag_denominator = ++residue_den;
			}

			D(cout << slice_wrapper_unit.window_start_time << " " << slice_wrapper_unit.completeness_tag_numerator << "/" << slice_wrapper_unit.completeness_tag_denominator << " " << slice_wrapper_unit.slice_id << endl;)
			memcpy(outMessage->buffer, inMessage->buffer, offset);  // simply copy header from old message for now!
			memcpy(outMessage->buffer + sizeof(int),
					&slice_wrapper_unit, sizeof(WrapperUnit)); 
			outMessage->size += offset;

			int event_count = (inMessage->size - offset) / sizeof(EventDG);

			D(cout << "THROUGHPUT: " << event_count <<" @RANK-"<<rank<<" TIME: "<<(long int)MPI_Wtime()<< endl;)

			THROUGHPUT_LOG(
					datafile <<event_count<< "\t"
					<<rank<< "\t"
					<<(long int)MPI_Wtime()
					<< endl;
					)


			int i = 0, j = 0;
			while (i < event_count) {

				sede.YSBdeserializeDG(inMessage, &eventDG,
						offset + (i * sizeof(EventDG)));

				D(cout << "  " << i << "\tevent_time: " << eventDG.event_time
						<< "\tevent_type: " << eventDG.event_type << "\t"
						<< "ad_id: " << eventDG.ad_id << endl;)

				sede.YSBserializeDG(&eventDG, outMessage);

				i++;
			}
			//cout << "FILTERED_EVENT_COUNT: " << j << endl;

			// Replicate data to all subsequent vertices, do not actually reshard the data here
			int n = 0;
			for (vector<Vertex*>::iterator v = next.begin(); v != next.end();
					++v) {

				int idx = n * worldSize + rank; // always keep workload on same rank

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
				break; // only one successor node allowed!
			}

			delete inMessage;
			c++;
		}

		tmpMessages->clear();
	}

	delete tmpMessages;
}
