//Copyright (C) 2011  Carl Rogers
//Released under MIT License
//license available in LICENSE file, or at http://www.opensource.org/licenses/mit-license.php

#ifndef LIBCNPY_H_
#define LIBCNPY_H_

#include <string>
#include <stdexcept>
#include <sstream>
#include <vector>
#include <cstdio>
#include <typeinfo>
#include <iostream>
#include <cassert>
#include <zlib.h>
#include <map>
#include <unordered_map>
#include <complex>
#include <cstdlib>
#include <algorithm>
#include <cstring>
#include <iomanip>

namespace cnpy {

struct NpyArray {
  char* data;
  unsigned int shape;
  unsigned int word_size;
  bool fortran_order;
  void destruct() {
    delete[] data;
  }
};

void parse_npy_header(FILE* fp,unsigned int& word_size, unsigned int*& shape, unsigned int& ndims, bool& fortran_order);
NpyArray npy_load(std::string fname);

template<typename T> std::vector<char>& operator+=(std::vector<char>& lhs, const T rhs) {
  //write in little endian
  for(char byte = 0; byte < sizeof(T); byte++) {
    char val = *((char*)&rhs+byte);
    lhs.push_back(val);
  }
  return lhs;
}

template<> std::vector<char>& operator+=(std::vector<char>& lhs, const std::string rhs);
template<> std::vector<char>& operator+=(std::vector<char>& lhs, const char* rhs);

template<typename T> std::string tostring(T i, int pad = 0, char padval = ' ') {
  std::stringstream s;
  s << i;
  return s.str();
}

template<> std::vector<char>& operator+=(std::vector<char>& lhs, const std::string rhs) {
  lhs.insert(lhs.end(),rhs.begin(),rhs.end());
  return lhs;
}

template<> std::vector<char>& operator+=(std::vector<char>& lhs, const char* rhs) {
  //write in little endian
  size_t len = strlen(rhs);
  lhs.reserve(len);
  for(size_t byte = 0; byte < len; byte++) {
    lhs.push_back(rhs[byte]);
  }
  return lhs;
}

void parse_npy_header(FILE* fp, unsigned int& word_size, unsigned int* shape, unsigned int& ndims, bool& fortran_order) {
  char buffer[256];
  size_t res = fread(buffer,sizeof(char),11,fp);
  if(res != 11)
    throw std::runtime_error("parse_npy_header: failed fread");
  std::string header = fgets(buffer,256,fp);
  assert(header[header.size()-1] == '\n');
  int loc1, loc2;
  //fortran order
  loc1 = header.find("fortran_order")+16;
  fortran_order = (header.substr(loc1,5) == "True" ? true : false);
  //shape
  loc1 = header.find("(");
  loc2 = header.find(")");
  std::string str_shape = header.substr(loc1+1,loc2-loc1-1);
  if(str_shape[str_shape.size()-1] == ',') ndims = 1;
  else ndims = std::count(str_shape.begin(),str_shape.end(),',')+1;
  assert(ndims == 1);
  for(unsigned int i = 0; i < ndims; i++) {
    loc1 = str_shape.find(",");
    shape[i] = atoi(str_shape.substr(0,loc1).c_str());
    str_shape = str_shape.substr(loc1+1);
  }
  //endian, word size, data type
  //byte order code | stands for not applicable.
  //not sure when this applies except for byte array
  loc1 = header.find("descr")+9;
  bool littleEndian = (header[loc1] == '<' || header[loc1] == '|' ? true : false);
  assert(littleEndian);
  //char type = header[loc1+1];
  //assert(type == map_type(T));
  std::string str_ws = header.substr(loc1+2);
  loc2 = str_ws.find("'");
  word_size = atoi(str_ws.substr(0,loc2).c_str());
}

NpyArray load_the_npy_file(FILE* fp) {
  unsigned int shape;
  unsigned int ndims, word_size;
  bool fortran_order;
  cnpy::parse_npy_header(fp,word_size,&shape,ndims,fortran_order);
  unsigned long long size = 1; //long long so no overflow when multiplying by word_size
  assert(ndims == 1);
  for(unsigned int i = 0; i < ndims; i++) size *= shape;
  cnpy::NpyArray arr;
  arr.word_size = word_size;
  arr.shape = shape;
  arr.data = new char[size*word_size];
  arr.fortran_order = fortran_order;
  size_t nread = fread(arr.data,word_size,size,fp);
  if(nread != size)
    throw std::runtime_error("load_the_npy_file: failed fread");
  return arr;
}

NpyArray npy_load(std::string fname) {
  FILE* fp = fopen(fname.c_str(), "rb");
  if(!fp) {
    printf("npy_load: Error! Unable to open file %s!\n",fname.c_str());
    abort();
  }
  NpyArray arr = load_the_npy_file(fp);
  fclose(fp);
  return arr;
}

NpyArray load_the_npy_file_to_buffer(FILE* fp, char **buffer, std::unordered_map<int, size_t> &buffer_len, int id) {
  unsigned int shape;
  unsigned int ndims, word_size;
  bool fortran_order;
  cnpy::parse_npy_header(fp,word_size,&shape,ndims,fortran_order);
  unsigned long long size = 1; //long long so no overflow when multiplying by word_size
  assert(ndims == 1);
  for(unsigned int i = 0; i < ndims; i++) size *= shape;
  cnpy::NpyArray arr;
  arr.word_size = word_size;
  arr.shape = shape;
  // arr.data = new char[size*word_size];
  if (buffer_len[id] < size*word_size) {
    if (buffer_len[id] > 0) {delete [] (*buffer);}
    buffer_len[id] = int(size*word_size*1.2);
    *buffer = new char[int(size*word_size*1.2)];
  }
  arr.data = *buffer;
  arr.fortran_order = fortran_order;
  size_t nread = fread(arr.data, word_size, size, fp);
  if(nread != size)
    throw std::runtime_error("load_the_npy_file: failed fread");
  return arr;
}

NpyArray npy_load_to_buffer(std::string fname, char **buffer, std::unordered_map<int, size_t> &buffer_len, int id) {
  FILE* fp = fopen(fname.c_str(), "rb");
  if(!fp) {
    printf("npy_load: Error! Unable to open file %s!\n",fname.c_str());
    abort();
  }
  NpyArray arr = load_the_npy_file_to_buffer(fp, buffer, std::ref(buffer_len), id);
  fclose(fp);
  return arr;
}

}

#endif
