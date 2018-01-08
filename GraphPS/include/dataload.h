#ifndef INCLUDE_DATALOAD_H_
#define INCLUDE_DATALOAD_H_

#include "include.h"

void clean_edge(int32_t p_id, char* data) {}

unsigned long get_file_size(const char *path)
{
    unsigned long filesize = -1;
    struct stat statbuff;
    if(stat(path, &statbuff) < 0){
        return filesize;
    }else{
        filesize = statbuff.st_size;
    }
    return filesize;
}

size_t GetDataSize(std::string dir_name) {
  DIR *d;
  struct dirent *de;
  struct stat buf;
  int exists;
  size_t total_size;
  d = opendir(dir_name.c_str());
  if (d == NULL) {assert(0 == 1);}
  total_size = 0;
  for (de = readdir(d); de != NULL; de = readdir(d)) {
    exists = stat((dir_name+de->d_name).c_str(), &buf);
    if (exists < 0) {
      fprintf(stderr, "Couldn't stat %s\n", de->d_name);
      assert(0 == 1);
    } else {
      total_size += buf.st_size;
    }
  }
  closedir(d);
  return total_size;
}

void Resize_Edge_Buffer(int32_t omp_id, size_t required_len) {
  if (_Edge_Buffer_Len[omp_id] < required_len
    || _Edge_Buffer_Len[omp_id] > required_len*1.2) {
    if (_Edge_Buffer_Len[omp_id] > 0) {
      delete [] (_Edge_Buffer[omp_id]);
    }
    _Edge_Buffer[omp_id] = new char[int(required_len*1.2)];
    _Edge_Buffer_Len[omp_id] = size_t(required_len*1.2);
  }
}

char* CacheHit_Level_1 (int32_t p_id, int omp_id) {
  size_t required_len = _EdgeCache[p_id].uncompressed_length;
  Resize_Edge_Buffer(omp_id, required_len);
  char *uncompressed = _Edge_Buffer[omp_id];
  assert (snappy::RawUncompress(_EdgeCache[p_id].data,
    _EdgeCache[p_id].compressed_length, uncompressed) == true);
  return uncompressed;
}

char* CacheHit_Level_2_3 (int32_t p_id, int omp_id) {
  size_t uncompressed_length = _EdgeCache[p_id].uncompressed_length;
  Resize_Edge_Buffer(omp_id, uncompressed_length);
  char *uncompressed = _Edge_Buffer[omp_id];
  int uncompress_result = 0;
  uncompress_result = uncompress((Bytef *)uncompressed, &uncompressed_length,
    (Bytef *)_EdgeCache[p_id].data, _EdgeCache[p_id].compressed_length);
  assert (uncompress_result == Z_OK);
  return uncompressed;
}

void Resize_Uncompressed_Buffer(int32_t omp_id, size_t max_compressed_len) {
  if (_Uncompressed_Buffer_Len[omp_id] < max_compressed_len ||
    _Uncompressed_Buffer_Len[omp_id] > 1.2*max_compressed_len) {
    if (_Uncompressed_Buffer_Len[omp_id] > 0) {
      delete [] (_Uncompressed_Buffer[omp_id]);
    }
    _Uncompressed_Buffer[omp_id] = new char[int(max_compressed_len*1.2)];
    _Uncompressed_Buffer_Len[omp_id] = size_t(max_compressed_len*1.2);
  }
}

char* PutCache_Level_1 (int32_t omp_id, cnpy::NpyArray &npz, size_t* compressed_length) {
  size_t max_compressed_len = snappy::MaxCompressedLength(sizeof(int32_t)*npz.shape);
  Resize_Uncompressed_Buffer(omp_id, max_compressed_len);
  char* compressed_data_tmp = _Uncompressed_Buffer[omp_id];
  snappy::RawCompress(npz.data, sizeof(int32_t)*npz.shape,
    compressed_data_tmp,  compressed_length);
  return compressed_data_tmp;
}

char* PutCache_Level_2 (int32_t omp_id, cnpy::NpyArray &npz, size_t* compressed_length) {
  size_t buf_size = compressBound(sizeof(int32_t)*npz.shape);
  *compressed_length = buf_size;
  size_t max_compressed_len = buf_size;
  Resize_Uncompressed_Buffer(omp_id, max_compressed_len);
  char* compressed_data_tmp = _Uncompressed_Buffer[omp_id];
  int compress_result = 0;
  compress_result = compress2((Bytef *)compressed_data_tmp,
    compressed_length, (Bytef *)npz.data,
    sizeof(int32_t)*npz.shape, 1);
  assert(compress_result == Z_OK);
  return compressed_data_tmp;
}

char* PutCache_Level_3 (int32_t omp_id, cnpy::NpyArray &npz, size_t* compressed_length) {
  size_t buf_size = compressBound(sizeof(int32_t)*npz.shape);
  *compressed_length = buf_size;
  size_t max_compressed_len = buf_size;
  Resize_Uncompressed_Buffer(omp_id, max_compressed_len);
  char* compressed_data_tmp = _Uncompressed_Buffer[omp_id];
  int compress_result = 0;
  compress_result = compress2((Bytef *)compressed_data_tmp,
    compressed_length, (Bytef *)npz.data,
    sizeof(int32_t)*npz.shape, 3);
  assert(compress_result == Z_OK);
  return compressed_data_tmp;
}

char *load_edge(int32_t p_id, std::string &DataPath) {
  int omp_id = omp_get_thread_num();
  assert(_Edge_Buffer_Lock[omp_id] == 0);
  _Edge_Buffer_Lock[omp_id]++;
  if (_EdgeCache.find(p_id) != _EdgeCache.end()) {
    char* uncompressed = NULL;
    if (COMPRESS_CACHE_LEVEL == 1) {
      uncompressed = CacheHit_Level_1(p_id, omp_id); 
    } else if (COMPRESS_CACHE_LEVEL == 2 || COMPRESS_CACHE_LEVEL == 3) {
      uncompressed = CacheHit_Level_2_3(p_id, omp_id); 
    } else if (COMPRESS_CACHE_LEVEL == 0){
      uncompressed = _EdgeCache[p_id].data;
    } else {assert(1 == 0);}
    _Edge_Buffer_Lock[omp_id]--;
    return uncompressed;
  }
  // Cannot find target data in cache
  _Missed_Num++;
  cnpy::NpyArray npz = cnpy::npy_load_to_buffer(
    DataPath, &_Edge_Buffer[omp_id], std::ref(_Edge_Buffer_Len), omp_id);
  std::srand(std::time(0));
  if (_EdgeCache_Size < EDGE_CACHE_SIZE
    && _EdgeCache.find(p_id) == _EdgeCache.end()) {
    EdgeCacheData newdata;
    char* compressed_data_tmp = NULL;
    char* compressed_data = NULL;
    size_t compressed_length = 0;

    if (COMPRESS_CACHE_LEVEL == 1) {
      compressed_data_tmp = PutCache_Level_1(omp_id, std::ref(npz), &compressed_length);
     } else if (COMPRESS_CACHE_LEVEL == 2) {
      compressed_data_tmp = PutCache_Level_2(omp_id, std::ref(npz), &compressed_length);
    } else if (COMPRESS_CACHE_LEVEL == 3) {
      compressed_data_tmp = PutCache_Level_3(omp_id, std::ref(npz), &compressed_length);
    } else if (COMPRESS_CACHE_LEVEL == 0) {
      newdata.data = new char[sizeof(int32_t)*npz.shape];
      memcpy(newdata.data, npz.data, sizeof(int32_t)*npz.shape);
      newdata.uncompressed_length = sizeof(int32_t)*npz.shape;
      newdata.compressed_length = sizeof(int32_t)*npz.shape;
    } else {
      assert (1 == 0);
    }
    if (COMPRESS_CACHE_LEVEL > 0) {
      compressed_data = new char[compressed_length];
      memcpy(compressed_data, compressed_data_tmp, compressed_length);
      newdata.data = compressed_data;
      newdata.compressed_length = compressed_length;
      newdata.uncompressed_length = sizeof(int32_t)*npz.shape;
    }
    _EdgeCache[p_id] = newdata;
    int32_t new_cache_size = std::ceil(newdata.compressed_length*1.0/1024/1024);
    _EdgeCache_Size.fetch_add(new_cache_size, std::memory_order_relaxed);
    new_cache_size = std::ceil(newdata.uncompressed_length*1.0/1024/1024);
    _EdgeCache_Size_Uncompress.fetch_add(new_cache_size, std::memory_order_relaxed);
  }
  _Edge_Buffer_Lock[omp_id]--;
  return _Edge_Buffer[omp_id];
}

#endif
