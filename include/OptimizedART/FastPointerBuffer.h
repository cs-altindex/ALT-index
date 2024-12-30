//
// Created by yyx on 2023/4/6.
//

#ifndef ALT_INDEX_FASTPOINTERBUFFER_H
#define ALT_INDEX_FASTPOINTERBUFFER_H

#include <iostream>
#include "N.h"
#include "../concurrency.h"

namespace ART_OLC {

    class FastPointerBuffer {

    public:
        FastPointerBuffer() {
            pointer_buffer.reserve(10000000);
        }

        ~FastPointerBuffer() {
            for(int i = 0 ; i < pointer_buffer.size() ; i++){
                delete &pointer_buffer[i];
            }
        }

        int insertFastPointer(N* pointer){
            for(int i = 0 ; i < pointer_buffer.size() ; i++){
                if(pointer == pointer_buffer[i].fast_pointer){
                    return i;
                }
            }
            pointer_buffer.push_back(FastPointerItem(pointer));
            return pointer_buffer.size() - 1;
        }

        struct FastPointerItem {
            N *fast_pointer;
            alt_index::spin_lock *lock_;

            FastPointerItem(N* pointer) : fast_pointer(pointer){
                lock_ = new alt_index::spin_lock();
            }

            ~FastPointerItem(){}
        };

        FastPointerItem getFastPointer(int pointerIndex) const {
            return pointer_buffer[pointerIndex];
        }

        int getFastPointerIndex(N* pointer){
            for(int i = 0 ; i < pointer_buffer.size() ; i++){
                if(pointer == pointer_buffer[i].fast_pointer){
                    return i;
                }
            }
            return pointer_buffer.size();
        }

        bool updateFastPointerWithIndex(int index, N* new_pointer){
            if(index >= pointer_buffer.size()){
                return false;
            }
            pointer_buffer[index].fast_pointer = new_pointer;
            return true;
        }

        bool isempty(){
            return pointer_buffer.size() == 0;
        }

        int size(){
            return pointer_buffer.size();
        }

        //get the match level of each fast pointer
        void pointerSavedPath(std::vector<uint64_t>& res){
            for(uint64_t i = 0 ; i < pointer_buffer.size() ; i++){
                res.push_back(pointer_buffer[i].fast_pointer->getMatchLevel());
            }
        }

    public:
        std::vector<FastPointerItem> pointer_buffer;
    };

}


#endif //ALT_INDEX_FASTPOINTERBUFFER_H
