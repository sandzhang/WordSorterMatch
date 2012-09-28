#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
//#include <vector>
#include <iostream>
#include <algorithm>
#include <functional>
#include <stdint.h>
#include <pthread.h>
#if __GNUC_PREREQ(4,3)
#else
#include <byteswap.h>
#endif

#include "thread.h"
#include "thread_store.h"

const int64_t MAX_BUF_LEN = 3L << 20;
const int64_t MAX_WORD_NUM = 300000L;
const int64_t MAX_THREAD_NUM = 16;
const int64_t MAX_WORD_LEN = 32;

int64_t microseconds() {
  struct timeval tv; 
  gettimeofday(&tv, NULL);
  return tv.tv_sec * 1000000L + tv.tv_usec;
}

int64_t get_file_size(int fd)
{    
  struct stat f_stat;
  if (fstat(fd, &f_stat) == -1)
  {
    return -1;
  }
  return (int64_t)f_stat.st_size;
} 

uint64_t bswap(uint64_t u)
{
#if __GNUC_PREREQ(4,3)
  return __builtin_bswap64(u);
#else
  return __bswap_64(u);
#endif
}

thread_store<std::string> output_buf;

class Word
{
  public:
    //Word() : w1_(0), w2_(0)/*, ptr_(NULL), len_(0)*/ {}
    Word() {}

    Word(const char * ptr, int64_t len)
        : w1_(0), w2_(0)
    {
      set_ptr(ptr, len);
    }

    Word(uint64_t w1, uint64_t w2, const char * ptr, const int64_t len) :
        w1_(w1), w2_(w2)/*, ptr_(ptr), len_(len)*/
    {
    }

    Word(const Word & r) :
        w1_(r.w1_), w2_(r.w2_)//,
//        ptr_(r.ptr_),
//        len_(r.len_)
    {}

    Word & operator = (const Word & r)
    {
      w1_ = r.w1_;
      w2_ = r.w2_;
//      ptr_ = r.ptr_;
//      len_ = r.len_;
      return *this;
    }

    void set_ptr(const char * ptr, int64_t len)
    {
//      ptr_ = ptr;
//      len_ = len;
      if (ptr[len - 1] == '\r') len--;
      const uint64_t * w = reinterpret_cast<const uint64_t *>(ptr);
      w1_ = w[0];
      w1_ = bswap(w1_);
      if (len <= 8)
      {
        w1_ &= (~0L) << ((8 - len) * 8);
      }
      else
      {
        w2_ = w[1];
        w2_ = bswap(w2_);
        w2_ &= (~0L) << ((16 - len) * 8);
      }
    }

    bool operator < (const Word & r) const
    {
      return w1_ < r.w1_ || (w1_ == r.w1_ && w2_ < r.w2_);
    }

    void get_ptr(char * p, int64_t & len) const
    {
      if (p != NULL)
      {
        len = get_len();
        uint64_t * n = reinterpret_cast<uint64_t *>(p);

        if (len <= 8)
        {
          uint64_t w1 = bswap(w1_);
          n[0] = w1;
        }
        else
        {
          uint64_t w1 = bswap(w1_);
          uint64_t w2 = bswap(w2_);
          n[0] = w1;
          n[1] = w2;
        }
      }
    }

    int64_t get_len() const
    {
      if (w2_ == 0)
      {
        uint64_t w1 = bswap(w1_);
        const uint8_t * p1 = reinterpret_cast<const uint8_t *>(&w1);
        if (p1[4] == 0)
          if (p1[2] == 0)
            if (p1[1] == 0) return 1;
            else return 2;
          else
            if (p1[3] == 0) return 3;
            else return 4;
        else
          if (p1[6] == 0)
            if (p1[5] == 0) return 5;
            else return 6;
          else
            if (p1[7] == 0) return 7;
            else return 8;
      }
      else
      {
        uint64_t w2 = bswap(w2_);
        const uint8_t * p2 = reinterpret_cast<const uint8_t *>(&w2);
        if (p2[4] == 0)
          if (p2[2] == 0)
            if (p2[1] == 0) return 9;
            else return 10;
          else
            if (p2[3] == 0) return 11;
            else return 12;
        else
          if (p2[6] == 0)
            if (p2[5] == 0) return 13;
            else return 14;
          else
            if (p2[7] == 0) return 15;
            else return 16;
      }
    }

  protected:
    uint64_t w1_;
    uint64_t w2_;
//    const char * ptr_;
//    int64_t len_;
};

typedef Word *   TWordList;
typedef Word *   TListIter;

Word words[2][MAX_WORD_NUM];

struct ListRange
{
  ListRange() {}
  ListRange(const TListIter & b, const TListIter & e)
    : begin(b), end(e) {}
  int64_t size() const
  {
    return end - begin;
  }
  TListIter begin;
  TListIter end;
};
typedef ListRange          TListRange;

class DWordList
{
  public:
    DWordList() : cur_index_(0) {}

    TWordList get_cur_list()
    {
      return words[cur_index_];
    }

    TWordList get_2nd_list()
    {
      return words[1 - cur_index_];
    }

    void switch_index()
    {
      cur_index_ = 1 - cur_index_;
    }

  protected:
    int cur_index_;
};

class SplitThread : public Thread
{
  public:
    const char * buf;
    int64_t len;
    int64_t start_offset;
    int64_t end_offset;
    Word res_list[MAX_WORD_NUM];
    int64_t res_num;
  public:
    void * run(void * arg)
    {
      if (0 != start_offset && buf[start_offset] != '\n')
      {
        while (start_offset < len && buf[start_offset] != '\n')
          start_offset++;
      }
      while (end_offset < len && buf[end_offset] != '\n')
        end_offset++;
      if (buf[start_offset] == '\n') start_offset++;
      res_num = 0;
      int64_t start = start_offset;
      for (int64_t i = start_offset; i < end_offset; i++)
      {
        if (buf[i] == '\n')
        {
          if (i > start)
          {
            res_list[res_num++].set_ptr(buf + start, i - start);
          }
          start = i + 1;
        }
      }
      if (start < end_offset)
      {
        res_list[res_num++].set_ptr(buf + start, end_offset - start);
      }
      std::sort(res_list, res_list + res_num);
      //std::cout << "ST: ";
      //print_words(res_list.begin(), res_list.end());
    }
};

class ListAcceptor
{
  public:
    ListAcceptor(TListIter & begin) : cur_(begin) {}
    void accept(TListIter & iter)
    {
      *cur_ = *iter;
      ++cur_;
    }
  protected:
    TListIter & cur_;
};

class Merger
{
  public:
    TListRange          ways[MAX_THREAD_NUM];
    int64_t             wnum;
  public:
    Merger() : wnum(0) {}

    void add_way(TListRange & range)
    {
      ways[wnum++] = range;
    }
    static bool merge_comp(const TListRange & l, const TListRange & r)
    {
      if (l.begin >= l.end) return true;
      if (r.begin >= r.end) return false;
      return !(*l.begin < *r.begin);
    }
    void merge(ListAcceptor & acceptor)
    {
      if (wnum == 1)
      {
        for (TListIter iter = ways[0].begin;
            iter != ways[0].end; iter++)
        {
          //acceptor.accept(*iter);
          acceptor.accept(iter);
        }
      }
      else if (wnum == 2)
      {
        int64_t total_len = (ways[0].end - ways[0].begin)
          + (ways[1].end - ways[1].begin);
        for (int64_t i = 0; i < total_len; i++)
        {
          if (ways[0].begin != ways[0].end)
          {
            if (ways[1].begin != ways[1].end)
            {
              if ((*ways[0].begin) < (*ways[1].begin))
              //if (ways[0].begin.less(ways[1].begin))
              {
                //acceptor.accept(*ways[0].begin++);
                acceptor.accept(ways[0].begin);
                ++ways[0].begin;
              }
              else
              {
                //acceptor.accept(*ways[1].begin++);
                acceptor.accept(ways[1].begin);
                ways[1].begin++;
              }
            }
            else
            {
              //acceptor.accept(*ways[0].begin++);
              acceptor.accept(ways[0].begin);
              ++ways[0].begin;
            }
          }
          else
          {
            if (ways[1].begin != ways[1].end)
            {
              //acceptor.accept(*ways[1].begin++);
              acceptor.accept(ways[1].begin);
              ++ways[1].begin;
            }
            else
            {
              // not expected
              throw new std::exception();
            }
          }
        }
      }
      else
      {
        std::make_heap(ways, ways + wnum, merge_comp);
        while (!is_end())
        {
          //acceptor.accept(*ways[0].begin);
          acceptor.accept(ways[0].begin);
          ways[0].begin++;
          std::make_heap(ways, ways + wnum, merge_comp);
        }
      }
    }
    bool is_end() const
    {
      return ways[0].begin >= ways[0].end;
    }
};

class MergeThread : public Thread
{
  public:
    TListRange  range1;
    TListRange  range2;
    TListIter   dest;
  public:
    void * run(void * arg)
    {
      Merger merger;
      merger.add_way(range1);
      merger.add_way(range2);
      ListAcceptor acceptor(dest);
      merger.merge(acceptor);
    }
};

class ConcatThread : public Thread
{
  public:
    TListIter begin_, end_;
    char      buf_[MAX_BUF_LEN];
    int64_t   len_;
    int64_t   file_size_;

  public:
    void * run(void * arg)
    {
      char word[MAX_WORD_LEN];
      int64_t len = 0;
      for (TListIter iter = begin_; iter != end_; iter++)
      {
        iter->get_ptr(word, len);
        memcpy(buf_ + len_, word, len);
        len_ += iter->get_len();
        //memcpy(buf_ + len_, iter.get_ptr(), iter.get_len());
        //len_ += iter.get_len();
        buf_[len_] = '\n';
        len_ ++; 
      }
    }

    void init(TListIter begin, TListIter end, int64_t file_size)
    {
      begin_ = begin;
      end_ = end;
      file_size_ = file_size;
      len_ = 0;
    }

    char * get_buf()
    {
      return buf_;
    }

    int64_t get_len()
    {
      return len_;
    }

};

SplitThread st[MAX_THREAD_NUM];
MergeThread mt[MAX_THREAD_NUM];
ConcatThread ct[MAX_THREAD_NUM];

class WordSorter
{
  protected:
    std::string         input_file_;
    std::string         output_file_;
    int                 thread_num_;
    int                 fd_;
    char                buf_[MAX_BUF_LEN];
    int64_t             buf_len_;
    int64_t             file_size_;
    int64_t             word_num_;
    DWordList           list_;
  public:
    WordSorter()
      : thread_num_(0), input_file_(), output_file_(),
        fd_(-1), buf_len_(0), file_size_(0), word_num_(0)
    {
    }

    void load_and_sort(int thread_num, const std::string & input_file,
        const std::string & output_file)
    {
      thread_num_ = thread_num;
      input_file_ = input_file;
      output_file_ = output_file;

      fd_ = open(input_file.c_str(), O_RDONLY);
      if (-1 == fd_)
      {
        throw new std::exception();
      }

      file_size_ = get_file_size(fd_);
      if (-1 == file_size_)
      {
        throw new std::exception();
      }
      file_size_ += 10;

      buf_len_ = read(fd_, buf_, file_size_);
      if (-1 == buf_len_)
      {
        throw new std::exception();
      }

      word_num_ = atol(buf_);
      std::cout << "Total words: " << word_num_ << std::endl;
      if (word_num_ > MAX_WORD_NUM)
      {
        std::cerr << "max words processed is " << MAX_WORD_NUM << std::endl;
        abort();
      }

      threaded_split_and_sort_words();
    }

    void threaded_split_and_sort_words() {
      if (0 == word_num_) return;
      int64_t num_per_thread = buf_len_ / thread_num_;
      for (int i = 0; i < thread_num_; i++)
      {
        st[i].buf = buf_;
        st[i].len = buf_len_;
        st[i].start_offset = i * num_per_thread;
        st[i].end_offset = (i + 1) * num_per_thread;
        if (i == thread_num_ - 1)
        {
          st[i].end_offset = buf_len_;
        }
        if (0 == i)
          while (buf_[st[0].start_offset] != '\n') st[0].start_offset++;
        st[i].start();
      }
      for (int i = 0; i < thread_num_; i++)
      {
        st[i].wait();
      }

      int64_t merge_thread_num = thread_num_ / 2;
      TListRange range_array[merge_thread_num];
      TWordList dest_list = list_.get_cur_list();
      TListIter dest_iter = dest_list;
      for (int64_t i = 0; i < merge_thread_num; i++)
      {
        int64_t sidx1 = i * 2;
        int64_t sidx2 = i * 2 + 1;
        TWordList list1 = st[sidx1].res_list;
        TWordList list2 = st[sidx2].res_list;
        TListRange range1(list1, list1 + st[sidx1].res_num);
        TListRange range2(list2, list2 + st[sidx2].res_num);
        mt[i].range1 = range1;
        mt[i].range2 = range2;
        mt[i].dest   = dest_iter;
        TListIter b = dest_iter;
        dest_iter += range1.size() + range2.size();
        range_array[i].begin = b;
        range_array[i].end = dest_iter;
        mt[i].start();
      }
      for (int64_t i = 0; i < merge_thread_num; i++)
      {
        mt[i].wait();
      }

      int64_t split_num = thread_num_ / 2;
      while (split_num > 1)
      {
        int64_t merge_num = split_num / 2;
        TWordList dest_list = list_.get_2nd_list();
        TListIter dest_iter = dest_list;
        for (int64_t i = 0; i < merge_num; i++)
        {
          int64_t sidx1 = i * 2;
          int64_t sidx2 = i * 2 + 1;
          mt[i].range1 = range_array[sidx1];
          mt[i].range2 = range_array[sidx2];
          mt[i].dest   = dest_iter;
          TListIter b = dest_iter;
          dest_iter += mt[i].range1.size() + mt[i].range2.size();
          range_array[i].begin = b;
          range_array[i].end = dest_iter;
          mt[i].start();
        }
        for (int64_t i = 0; i < merge_num; i++)
        {
          mt[i].wait();
          //tpool_.get_output();
        }
        split_num = merge_num;
        list_.switch_index();
      }

    }

    void write2output()
    {
      if (0 == word_num_) return;
      int ofd = open(output_file_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
      if (ofd == -1)
      {
        throw new std::exception();
      }

      char * obuf = static_cast<char *>(malloc(file_size_));
      if (NULL == obuf)
      {
        throw new std::exception();
      }

      int64_t s0 = microseconds();

      int64_t num_per_thread = word_num_ / thread_num_;
      for (int i = 0; i < thread_num_; i++)
      {
        TListIter begin = list_.get_cur_list() + i * num_per_thread;
        TListIter end = list_.get_cur_list() + (i + 1) * num_per_thread;
        if (i == thread_num_ - 1)
        {
          end = list_.get_cur_list() + word_num_;
        }
        ct[i].init(begin, end, file_size_);
        ct[i].start();
      }
      for (int i = 0; i < thread_num_; i++)
      {
        ct[i].wait();
      }
      int64_t s1 = microseconds();
      std::cout << "concating used: " << s1 - s0 << std::endl;

      for (int i = 0; i < thread_num_; i++)
      {
        int size = write(ofd, ct[i].get_buf(), ct[i].get_len());
        if (size == -1)
        {
          throw new std::exception();
        }
      }

      close(ofd);
    }
};

void usage(const char * prog_name)
{
  std::cerr << "\nusage: " << prog_name
    << " <thread number> <input file> <output file>\n" << std::endl;;
}

WordSorter ws;

int main(int argc, char * argv[])
{
  if (argc < 4)
  {
    usage(argv[0]);
    exit(1);
  }
  int thread_num = atoi(argv[1]);
  if (thread_num == 0 || (thread_num & (thread_num - 1) != 0))
  {
    std::cerr << "thread number must be power of 2" << std::endl;
    usage(argv[0]);
    exit(2);
  }
  else if (thread_num > MAX_THREAD_NUM)
  {
    std::cerr << "thread number must be less than " << MAX_THREAD_NUM << std::endl;
    exit(2);
  }
  int64_t s0 = microseconds();
  ws.load_and_sort(thread_num, argv[2], argv[3]);
  int64_t s1 = microseconds();
  ws.write2output();
  int64_t s2 = microseconds();
  std::cout << "Loading and Sorting: " << s1 - s0 << "us\n";
  std::cout << "Writing: " << s2 - s1 << "us\n";
  std::cout << "Total elapsed time: " << s2 - s0 << "us" << std::endl;;
  return 0;
}

