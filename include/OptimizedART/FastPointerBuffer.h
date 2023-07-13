//
// Created by admin on 2023/4/6.
//

#ifndef ALT_INDEX_FASTPOINTERBUFFER_H
#define ALT_INDEX_FASTPOINTERBUFFER_H

#include<iostream>
#include "N.h"

using namespace ART;

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
            FastPointerItem(N* pointer) : fast_pointer(pointer){}
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

    public:
        std::vector<FastPointerItem> pointer_buffer;

    };

}


#endif //ALT_INDEX_FASTPOINTERBUFFER_H
