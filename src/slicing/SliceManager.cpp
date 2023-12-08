#ifndef SLICE_M
#define SLICE_M

#include <queue>
#include<vector>
#include <utility>
#include<iostream>
#include <unordered_map>
#include <set>
#include <mpi.h>

using namespace std;


class PairedWindow {
    private:
        int queryID;
        int r, s, a, b;
        long int start_time;
    public:
        PairedWindow(int queryID, int r, int s, long int start){
            this->queryID = queryID;
            this->r = r;
            this->s = s;
            this->a = r % s;
            this->b = s - this->a; 
            this->start_time = start;
            
        }
        int get_a(){
            return this->a;
        }
        int get_b(){
            return this->b;
        }

        int get_r(){
            return this->r;
        }

        int get_s(){
            return this->s;
        }

        int get_id(){
            return this->queryID;
        }
        long int get_start(){
            return this->start_time;
        }
};

class Edge{
    public:
        int time;
        PairedWindow* window;
        bool last;

        Edge(int time, PairedWindow* w, bool last){
            this->time = time;
            this->window = w;
            this->last = last;

        }

};

struct Comp{
    bool operator()(const Edge* a, const Edge* b) const {
        return a->time < b->time;
    }
};

class SliceManager {
    private:
        // priority_queue<Edge*, vector<Edge*>, Comp> pq;
        multiset<Edge*, Comp> st;
    public: 
        SliceManager(unordered_map<int, PairedWindow*> windows, int start_time){
            for (auto it = windows.begin(); it != windows.end(); it++){
                add_edges(start_time, it->second);
            }
        }

        void add_edges(int start_time, PairedWindow *w){
            // pq.push(new Edge(start_time + w->get_a(), w, false));
            // pq.push(new Edge(start_time + w->get_a() + w->get_b(), w, true));

            st.insert(new Edge(start_time + w->get_a(), w, false));
            st.insert(new Edge(start_time + w->get_a() + w->get_b(), w, true));
        }

        void remove_edges(int id){
            auto it = st.begin();
            while (it != st.end()){
                if ((*it)->window->get_id() == id) {
                    it = st.erase(it);
                }
                else {
                    ++it;
                }
            }       
        }

        int get_next_edge(){
            if (st.empty()){
                return -1;
            }
            Edge* ec;

            int tc = (*st.begin())->time;
            return tc;
        }

        int advance_window_get_next_edge(){
            // if (pq.empty()){
            //     return -1;
            // }
            // Edge* ec;
            // int tc = pq.top()->time;
            // while (!pq.empty() && tc == pq.top()->time){
            //     ec = pq.top(); pq.pop();
            //     if (ec->last){
            //         add_edges(ec->time, ec->window);
            //     }
            // }

            if (st.empty()){
                return -1;
            }
            Edge* ec;

            int tc = (*st.begin())->time;
            while (!st.empty() && tc == (*st.begin())->time){
                ec = *st.begin(); st.erase(st.begin());
                if (ec->last){
                    add_edges(ec->time, ec->window);
                }
            }

            return (*st.begin())->time;
        }
};

// int main(){
//     unordered_map<int, PairedWindow*> windows;
//     windows[1] =  new PairedWindow(1, 18, 15, 0);
//     windows[2] = new PairedWindow(2, 12, 10, 0);
//     SliceManager* sm = new SliceManager(windows, 0);
//     int i = 50;
//     while (i--){
//         cout << sm->get_next_edge() << endl;
//         sm->advance_window_get_next_edge();
//     }
// }

#endif // SLICE_M