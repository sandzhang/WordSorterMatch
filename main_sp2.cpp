#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <vector>
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
#include "ob_fixed_queue.h"
using namespace oceanbase::common;

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

class Runnable
{
  public:
    virtual void * run(void * arg) = 0;
};

class ThreadPool
{
  public:
    typedef ObFixedQueue<Runnable> TInputQueue;
    typedef ObFixedQueue<int64_t>  TOutputQueue;
    class BusyThread : public Thread
    {
      protected:
        TInputQueue  &    input_;
        TOutputQueue &    output_;
        pthread_cond_t &  cond_;
        pthread_mutex_t & mutex_;
        bool              stop_;
        int64_t           output_sign_;
      public:
        BusyThread(TInputQueue & input, TOutputQueue & output,
                   pthread_cond_t & cond, pthread_mutex_t & mutex)
          : input_(input), output_(output),
            cond_(cond), mutex_(mutex),
            stop_(false), output_sign_(1)
        {
        }

        BusyThread(const BusyThread & bt)
          : input_(bt.input_), output_(bt.output_),
            cond_(bt.cond_), mutex_(bt.mutex_),
            stop_(false), output_sign_(1)
        {
        }

        void * run(void * arg)
        {
          int err = 0;
          Runnable * runnable = NULL;
          while (!stop_)
          {
            pthread_mutex_lock(&mutex_);
            pthread_cond_wait(&cond_, &mutex_);
            pthread_mutex_unlock(&mutex_);
            //while ((err = input_.pop(runnable)) == -2 && !stop_);
            if (stop_) break;
            err = input_.pop(runnable);
            if (err == -2) continue;
            //printf("pop %p  err = %d\n", runnable, err);
            if (0 != err)
            {
              abort();
            }
            else if (NULL == runnable)
            {
              abort();
            }
            else
            {
              runnable->run(NULL);
              output_.push(&output_sign_);
            }
          }
        }

        void stop()
        {
          stop_ = true;
        }
    };

  protected:
    int64_t thread_num_;
    TInputQueue  input_;
    TOutputQueue output_;
    typedef std::vector<BusyThread> TThreadArray;
    typedef TThreadArray::iterator  TThreadIter;
    TThreadArray threads_;
    pthread_cond_t cond_;
    pthread_mutex_t mutex_;

  public:
    ThreadPool(int64_t thread_num)
      : thread_num_(thread_num),
        threads_(thread_num, BusyThread(input_, output_, cond_, mutex_))
    {
      pthread_mutex_init(&mutex_, NULL);
      pthread_cond_init(&cond_, NULL);
      input_.init(thread_num_ * 2);
      output_.init(thread_num_ * 2);
      for (TThreadIter iter = threads_.begin(); iter != threads_.end(); iter++)
      {
        iter->start();
      }
    }

    ~ThreadPool()
    {
      stop();
      pthread_cond_destroy(&cond_);
      pthread_mutex_destroy(&mutex_);
    }

    void stop()
    {
      for (TThreadIter iter = threads_.begin(); iter != threads_.end(); iter++)
      {
        iter->stop();
      }
      pthread_cond_broadcast(&cond_);
      for (TThreadIter iter = threads_.begin(); iter != threads_.end(); iter++)
      {
        iter->wait();
      }
    }

    void add_runnable(Runnable * runnable)
    {
      //printf("add %p\n", runnable);
      input_.push(runnable);
      pthread_cond_signal(&cond_);
    }

    int64_t * get_output()
    {
      int err = 0;
      int64_t * ret = 0;
      while ((err = output_.pop(ret)) == -2);
      if (0 != err)
      {
        abort();
      }
      else
      {
        return ret;
      }
    }
};

class WordList;
class Word
{
    friend class WordList;
  public:
    Word() : w1_(0), w2_(0), /*ptr_(NULL), */len_(0) {}

    Word(const char * ptr, const int64_t len)
        : w1_(0), w2_(0)
    {
//      ptr_ = ptr;
      len_ = len;
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
      //printf("%lx %lx %lx %lx %lx\n", w1_, w2_, (~0L) << ((8 - len) * 8), w[0], w[1]);

//          int64_t m = std::min(len, 8L);
//          for (int64_t i = 0; i < m; i++)
//          {
//            w1_ |= static_cast<int64_t>((ptr[i] - 'a')) << ((7 - i) * 5);
//          }
//          if (len > 8)
//          {
//            for (int64_t i = 8; i < len; i++)
//            {
//              w2_ |= static_cast<int64_t>((ptr[i] - 'a')) << ((15 - i) * 5);
//            }
//          }
    }

    Word(uint64_t w1, uint64_t w2, const char * ptr, const int64_t len) :
        w1_(w1), w2_(w2), /*ptr_(ptr), */len_(len)
    {
    }

    Word(const Word & r) :
        w1_(r.w1_), w2_(r.w2_),
//        ptr_(r.ptr_),
        len_(r.len_)
    {}

    Word & operator = (const Word & r)
    {
      w1_ = r.w1_;
      w2_ = r.w2_;
//      ptr_ = r.ptr_;
      len_ = r.len_;
      return *this;
    }

    bool operator < (const Word & r) const
    {
      return w1_ < r.w1_ || (w1_ == r.w1_ && w2_ < r.w2_);
    }

    const char* get_ptr() const
    {
      std::string * p = output_buf.get();
      if (p != NULL)
      {
        p->clear();

        if (len_ <= 8)
        {
          uint64_t w1 = bswap(w1_);
          p->append(reinterpret_cast<const char *>(&w1), len_);
        }
        else
        {
          uint64_t w1 = bswap(w1_);
          uint64_t w2 = bswap(w2_);
          p->append(reinterpret_cast<const char *>(&w1), 8);
          p->append(reinterpret_cast<const char *>(&w2), len_ - 8);
        }
        return p->c_str();
      }
      else
      {
        return NULL;
      }
    }

    int64_t get_len() const
    {
      return len_;
    }

    void print() const
    {
      std::cout << get_ptr();
    }

  protected:
    uint64_t w1_;
    uint64_t w2_;
//    const char * ptr_;
    int64_t len_;
};

class WordList
{
  protected:
    uint64_t * ls_;
    uint64_t * rs_;
    const char ** ptrs_;
    int64_t * lens_;
    int64_t size_;
    int64_t cap_;

    void init_(int64_t cap)
    {
      ls_ = new uint64_t[cap];
      rs_ = new uint64_t[cap];
      ptrs_ = new const char * [cap];
      lens_ = new int64_t[cap];
      size_ = 0;
      cap_ = cap;
    }

  public:
    static const int64_t DEFAULT_LEN = 300000;

    WordList()
    {
      init_(DEFAULT_LEN);
    }

    WordList(int64_t cap)
    {
      init_(cap);
    }

    ~WordList()
    {
      delete[] ls_;
      delete[] rs_;
      delete[] ptrs_;
      delete[] lens_;
    }

    int64_t size() const
    {
      return size_;
    }

    void reserve(int64_t cap)
    {
      if (cap > cap_)
      {
        delete[] ls_;
        delete[] rs_;
        delete[] ptrs_;
        delete[] lens_;
        init_(cap);
      }
    }

    void resize(int64_t size)
    {
      reserve(size);
      size_ = size;
    }

    void push_back(const Word & w)
    {
      if (size_ >= cap_)
      {
        throw new std::exception();
      }
      else
      {
        ls_[size_] = w.w1_;
        rs_[size_] = w.w2_;
        //ptrs_[size_] = w.ptr_;
        lens_[size_] = w.len_;
        size_ ++;
      }
    }

    Word operator [] (int64_t index) const
    {
      if (index < 0 && index >= size_)
      {
        throw new std::exception();
      }
      return Word(ls_[index], rs_[index], ptrs_[index], lens_[index]);
    }

    class iterator
    {
      public:
        typedef std::random_access_iterator_tag iterator_category;
        typedef Word                          value_type;
        typedef int64_t                       difference_type;
        typedef Word *                        pointer;
        typedef Word &                        reference;
      public:
        iterator() : index_(0) {}
        iterator(int64_t index, WordList & list)
          : index_(index), plist_(&list) {}

        iterator & operator ++ ()
        {
          index_++;
          return *this;
        }

        iterator operator ++ (int)
        {
          int64_t o = index_;
          index_++;
          return iterator(o, *plist_);
        }

        iterator & operator -- ()
        {
          index_--;
          return *this;
        }

        iterator operator -- (int)
        {
          int64_t o = index_;
          index_--;
          return iterator(o, *plist_);
        }

        iterator & operator += (int64_t s)
        {
          index_ += s;
          return *this;
        }

        iterator operator + (int64_t s)
        {
          if (plist_ == NULL) throw new std::exception();
          return iterator(index_ + s, *plist_);
        }

        iterator operator - (int64_t s) const
        {
          if (plist_ == NULL) throw new std::exception();
          return iterator(index_ - s, *plist_);
        }

        int64_t operator - (const iterator & r) const
        {
          if (plist_ != r.plist_) throw new std::exception();
          return r.index_ - index_;
        }

        bool operator >= (const iterator & r) const
        {
          if (plist_ != r.plist_) throw new std::exception();
          return index_ >= r.index_;
        }

        bool operator < (const iterator & r) const
        {
          if (plist_ != r.plist_) throw new std::exception();
          return  index_ < r.index_;
        }

        bool operator != (const iterator & r) const
        {
          if (plist_ != r.plist_) throw new std::exception();
          return r.index_ != index_;
        }

        bool operator == (const iterator & r) const
        {
          if (plist_ != r.plist_) throw new std::exception();
          return r.index_ == index_;
        }

        //void operator = (const Word & w)
        //{
        //  if (NULL == plist_) throw new std::exception();
        //  plist_->ls_[index_] = w.w1_;
        //  plist_->rs_[index_] = w.w2_;
        //  //plist_->ptrs_[index_] = w.ptr_;
        //  plist_->lens_[index_] = w.len_;
        //}

        void copy_from(const iterator & r)
        {
          if (NULL == plist_ || NULL == r.plist_) throw new std::exception();
          plist_->ls_[index_] = r.plist_->ls_[r.index_];
          plist_->rs_[index_] = r.plist_->rs_[r.index_];
          //plist_->ptrs_[index_] = r.plist_->ptrs_[r.index_];
          plist_->lens_[index_] = r.plist_->lens_[r.index_];
        }

        const char* get_ptr() const
        {
          return plist_->ptrs_[index_];
        }

        int64_t get_len() const
        {
          return plist_->lens_[index_];
        }

        bool less(const iterator & r) const
        {
          if (plist_ == NULL || r.plist_ == NULL) throw new std::exception();
          return plist_->ls_[index_] < r.plist_->ls_[r.index_]
                 || (plist_->ls_[index_] == r.plist_->ls_[r.index_]
                     && plist_->rs_[index_] < r.plist_->rs_[r.index_]);
        }

        void print()
        {
        }

        Word operator * () const
        {
          return (*plist_)[index_];
        }

      protected:
        int64_t index_;
        WordList * plist_;
    };

    iterator begin()
    {
      return iterator(0, *this);
    }

    iterator end()
    {
      return iterator(size_, *this);
    }
};

typedef std::vector<Word>   TWordList;
//typedef WordList            TWordList;
typedef TWordList::iterator TListIter;
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
typedef std::vector<TListRange> TRangeArray;

void print_words(TListIter begin, TListIter end)
{
  for (TListIter iter = begin; iter != end; iter++)
  {
    //(*iter).print();
    std::cout << " | " ;
  }
  std::cout << std::endl;
}

//struct MergeParam
//{
//  TListIter begin;
//  TListIter end;
//  TListIter begin2;
//  TListIter end2;
//  TListIter dest;
//};
//typedef std::vector<MergeParam> TMergeParams;

class DWordList
{
  public:
    DWordList() : cur_index_(0) {}

    TWordList & get_cur_list()
    {
      return word_list_[cur_index_];
    }

    TWordList & get_2nd_list()
    {
      return word_list_[1 - cur_index_];
    }

    //TMergeParams & get_cur_param()
    //{
    //  return merge_params_[cur_index_];
    //}

    //TMergeParams & get_2nd_param()
    //{
    //  return merge_params_[1 - cur_index_];
    //}

    void switch_index()
    {
      cur_index_ = 1 - cur_index_;
    }

  protected:
    TWordList word_list_[2];
    //TMergeParams merge_params_[2];
    int cur_index_;
};

class SplitThread : public Thread
//class SplitThread : public Runnable
{
  public:
    const char * buf;
    int64_t len;
    int64_t start_offset;
    int64_t end_offset;
    TWordList res_list;
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
      int64_t start = start_offset;
      for (int64_t i = start_offset; i < end_offset; i++)
      {
        //if (isspace(buf[i]))
        if (buf[i] == '\n')
        {
          if (i > start)
          {
            res_list.push_back(Word(buf + start, i - start));
          }
          start = i + 1;
        }
      }
      if (start < end_offset)
      {
        res_list.push_back(Word(buf + start, end_offset - start));
      }
      std::sort(res_list.begin(), res_list.end());
      //std::cout << "ST: ";
      //print_words(res_list.begin(), res_list.end());
    }
};

//bool sort_comp(const Word & l, const Word & r)
//{
//  return l < r;
//}
//
//class SortThread : public Thread
//{
//  public:
//    TListIter begin;
//    TListIter end;
//  public:
//    void * run(void * arg)
//    {
//      MergeParam * param = reinterpret_cast<MergeParam *>(arg);
//      std::sort(param->begin, param->end);
//    }
//};

class ListAcceptor
{
  public:
    ListAcceptor(TListIter & begin) : cur_(begin) {}
    //void accept(Word & word)
    //{
    //  *cur_++ = word;
    //}
    void accept(TListIter & iter)
    {
      *cur_ = *iter;
      //cur_.copy_from(iter);
      ++cur_;
    }
  protected:
    TListIter & cur_;
};

//class FileAcceptor : public Acceptor
//{
//  public:
//    FileAcceptor(const std::string & filename, int64_t file_size) : fd_(-1), buf_index_(0)
//    {
//      fd_ = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
//      if (fd_ == -1)
//      {
//        throw new std::exception();
//      }
//      buf_ = static_cast<char *>(malloc(file_size));
//      if (NULL == buf_)
//      {
//        throw new std::bad_alloc();
//      }
//    }
//    ~FileAcceptor()
//    {
//      if (-1 == write(fd_, buf_, buf_index_))
//      {
//        abort();
//      }
//      close(fd_);
//    }
//    void accept(Word & word)
//    {
//      memcpy(buf_ + buf_index_, word.get_ptr(), word.get_len());
//      buf_index_ += word.get_len();
//      buf_[buf_index_] = '\n';
//      buf_index_ ++; 
//    }
//  protected:
//    int fd_;
//    char * buf_;
//    int64_t buf_index_;
//};

class Merger
{
  public:
    typedef std::vector<TListRange>  MergerWays;
    MergerWays          ways;
  public:
    void add_way(TListRange & range)
    {
      ways.push_back(range);
    }
    static bool merge_comp(const TListRange & l, const TListRange & r)
    {
      if (l.begin >= l.end) return true;
      if (r.begin >= r.end) return false;
      return !(*l.begin < *r.begin);
    }
    void merge(ListAcceptor & acceptor)
    {
      if (ways.size() == 1)
      {
        for (TListIter iter = ways[0].begin;
            iter != ways[0].end; iter++)
        {
          //acceptor.accept(*iter);
          acceptor.accept(iter);
        }
      }
      else if (ways.size() == 2)
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
        make_heap(ways.begin(), ways.end(), merge_comp);
        while (!is_end())
        {
          //acceptor.accept(*ways[0].begin);
          acceptor.accept(ways[0].begin);
          ways[0].begin++;
          make_heap(ways.begin(), ways.end(), merge_comp);
        }
      }
    }
    bool is_end() const
    {
      return ways[0].begin >= ways[0].end;
    }
};

class MergeThread : public Thread
//class MergeThread : public Runnable
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
//class ConcatThread : public Runnable
{
  public:
    TListIter begin_, end_;
    char * buf_;
    int64_t len_;
    int64_t file_size_;

  public:
    void * run(void * arg)
    {
      for (TListIter iter = begin_; iter != end_; iter++)
      {
        memcpy(buf_ + len_, iter->get_ptr(), iter->get_len());
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

      buf_ = static_cast<char *>(malloc(file_size));
      if (NULL == buf_)
      {
        throw new std::exception();
      }
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

class WordSorter
{
  protected:
    std::string         input_file_;
    std::string         output_file_;
    int                 thread_num_;
    int                 fd_;
    char *              buf_;
    int64_t             buf_len_;
    int64_t             file_size_;
    int64_t             word_num_;
    DWordList           list_;
    //ThreadPool          tpool_;
  public:
    WordSorter(int thread_num, const std::string & input_file,
        const std::string & output_file)
      : thread_num_(thread_num), input_file_(input_file),
        output_file_(output_file),
        fd_(-1), buf_(NULL), buf_len_(0), file_size_(0), word_num_(0)//,
        //tpool_(thread_num)
    {
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

      buf_ = static_cast<char *>(malloc(file_size_));
      if (NULL == buf_)
      {
        throw new std::bad_alloc();
      }

      buf_len_ = read(fd_, buf_, file_size_);
      if (-1 == buf_len_)
      {
        throw new std::exception();
      }

      word_num_ = atol(buf_);
      std::cout << "Total words: " << word_num_ << std::endl;
      list_.get_cur_list().resize(word_num_);
      list_.get_2nd_list().resize(word_num_);

      threaded_split_and_sort_words();
    }

    void threaded_split_and_sort_words() {
      if (0 == word_num_) return;
      int64_t num_per_thread = buf_len_ / thread_num_;
      SplitThread * st = new SplitThread[thread_num_];
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
        st[i].res_list.reserve(word_num_);
        if (0 == i)
          while (buf_[st[0].start_offset] != '\n') st[0].start_offset++;
        st[i].start();
        //tpool_.add_runnable(st + i);
      }
      for (int i = 0; i < thread_num_; i++)
      {
        st[i].wait();
        //tpool_.get_output();
      }

      int64_t merge_thread_num = thread_num_ / 2;
      MergeThread * mt = new MergeThread[merge_thread_num];
      TRangeArray range_array(merge_thread_num);
      TWordList & dest_list = list_.get_cur_list();
      TListIter dest_iter = dest_list.begin();
      for (int64_t i = 0; i < merge_thread_num; i++)
      {
        int64_t sidx1 = i * 2;
        int64_t sidx2 = i * 2 + 1;
        TWordList & list1 = st[sidx1].res_list;
        TWordList & list2 = st[sidx2].res_list;
        TListRange range1(list1.begin(), list1.end());
        TListRange range2(list2.begin(), list2.end());
        mt[i].range1 = range1;
        mt[i].range2 = range2;
        mt[i].dest   = dest_iter;
        TListIter b = dest_iter;
        dest_iter += range1.size() + range2.size();
        range_array[i].begin = b;
        range_array[i].end = dest_iter;
        mt[i].start();
        //tpool_.add_runnable(mt + i);
      }
      for (int64_t i = 0; i < merge_thread_num; i++)
      {
        mt[i].wait();
        //tpool_.get_output();
      }
      //print_words(dest_list.begin(), dest_list.end());

      int64_t split_num = thread_num_ / 2;
      while (split_num > 1)
      {
        int64_t merge_num = split_num / 2;
        TWordList & dest_list = list_.get_2nd_list();
        TListIter dest_iter = dest_list.begin();
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
          //tpool_.add_runnable(mt + i);
        }
        for (int64_t i = 0; i < merge_num; i++)
        {
          mt[i].wait();
          //tpool_.get_output();
        }
        split_num = merge_num;
        list_.switch_index();
      }

      //int64_t link_time = 0;
      //for (int i = 0; i < thread_num_; i++)
      //{
      //  st[i].wait();
      //  int64_t ss0 = microseconds();
      //  for (TListIter iter = st[i].res_list.begin();
      //       iter != st[i].res_list.end(); iter++)
      //  {
      //    list_.get_cur_list().push_back(*iter);
      //  }
      //  link_time += microseconds() - ss0;
      //}
      //int64_t s1 = microseconds();
      //std::cout << "splitting used: " << s1 - s0 << std::endl;
      //std::cout << "linking used: " << link_time << std::endl;
      //for (int i = 0; i < 9; i++)
      //{
      //  printf("len = %ld word = %s\n", list_.get_cur_list()[i].get_len(),
      //      list_.get_cur_list()[i].get_ptr());
      //}
    }

    //void threaded_sort(int64_t thread_num)
    //{
    //  if (0 == word_num_) return;
    //  SortThread * t = new SortThread[thread_num];
    //  list_.get_cur_param().resize(thread_num);
    //  int64_t word_per_thread = list_.get_cur_list().size() / thread_num;
    //  for (int64_t i = 0; i < thread_num - 1; i++)
    //  {
    //    list_.get_cur_param()[i].begin = list_.get_cur_list().begin() + i * word_per_thread;
    //    list_.get_cur_param()[i].end = list_.get_cur_list().begin() + (i + 1) * word_per_thread;
    //  }
    //  list_.get_cur_param()[thread_num - 1].begin = list_.get_cur_list().begin() + (thread_num - 1) * word_per_thread;
    //  list_.get_cur_param()[thread_num - 1].end = list_.get_cur_list().end();
    //  for (int64_t i = 0; i < thread_num; i++)
    //  {
    //    t[i].start(&(list_.get_cur_param()[i]));
    //  }
    //  for (int64_t i = 0; i < thread_num; i++)
    //  {
    //    t[i].wait();
    //  }

    //  int64_t split_num = thread_num;
    //  list_.get_2nd_list().resize(list_.get_cur_list().size());
    //  list_.get_2nd_param().resize(split_num);
    //  MergeThread * m = new MergeThread[split_num];
    //  //print_words(list_.get_cur_list().begin(), list_.get_cur_list().end());
    //  while (split_num > 1)
    //  {
    //    int64_t merge_num = split_num / 2;
    //    for (int64_t i = 0; i < merge_num; i++)
    //    {
    //      list_.get_2nd_param()[i].begin  = list_.get_cur_param()[i * 2].begin;
    //      list_.get_2nd_param()[i].end    = list_.get_cur_param()[i * 2].end;
    //      list_.get_2nd_param()[i].begin2 = list_.get_cur_param()[i * 2 + 1].begin;
    //      list_.get_2nd_param()[i].end2   = list_.get_cur_param()[i * 2 + 1].end;
    //      int64_t pos = list_.get_cur_param()[i * 2].begin - list_.get_cur_list().begin();
    //      //std::cout << "pos = " << pos << std::endl;
    //      list_.get_2nd_param()[i].dest   = list_.get_2nd_list().begin() + pos;
    //      m[i].start(&(list_.get_2nd_param()[i]));
    //      //m[i].wait();
    //      //std::cout << "begin = " << list_.get_2nd_param()[i].begin - list_.get_2nd_list().begin()
    //      //  << " , end = " << list_.get_2nd_param()[i].end - list_.get_2nd_list().begin()
    //      //  << std::endl;
    //    }
    //    if (split_num % 2 == 1)
    //    {
    //      list_.get_2nd_param()[merge_num].begin = list_.get_cur_param()[split_num - 1].begin;
    //      list_.get_2nd_param()[merge_num].end   = list_.get_cur_param()[split_num - 1].end;
    //      split_num = merge_num + 1;
    //      Merger merger;
    //      merger.add_way(list_.get_2nd_param()[merge_num]);
    //      TListIter ac = list_.get_2nd_list().begin()
    //          + (list_.get_cur_param()[merge_num].begin - list_.get_cur_list().begin());
    //      list_.get_2nd_param()[merge_num].begin = ac;
    //      ListAcceptor acceptor(ac);
    //      merger.merge(acceptor);
    //      list_.get_2nd_param()[merge_num].end = ac;
    //    }
    //    else
    //    {
    //      split_num = merge_num;
    //    }
    //    for (int64_t i = 0; i < merge_num; i++)
    //    {
    //      m[i].wait();
    //    }
    //    list_.switch_index();
    //    //print_words(list_.get_cur_list().begin(), list_.get_cur_list().end());
    //  }
    //  //print_words(list_.get_cur_list().begin(), list_.get_cur_list().end());
    //}

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
      //int64_t obufi = 0;
      //for (TListIter iter = list_.get_cur_list().begin();
      //    iter != list_.get_cur_list().end(); iter++)
      //{
      //  memcpy(obuf + obufi, (*iter)->get_ptr(), (*iter)->get_len());
      //  obufi += (*iter)->get_len();
      //  obuf[obufi] = '\n';
      //  obufi ++; 
      //}

      int64_t num_per_thread = list_.get_cur_list().size() / thread_num_;
      ConcatThread * ct = new ConcatThread[thread_num_];
      for (int i = 0; i < thread_num_; i++)
      {
        TListIter begin = list_.get_cur_list().begin() + i * num_per_thread;
        TListIter end = list_.get_cur_list().begin() + (i + 1) * num_per_thread;
        if (i == thread_num_ - 1)
        {
          end = list_.get_cur_list().end();
        }
        ct[i].init(begin, end, file_size_);
        ct[i].start();
        //tpool_.add_runnable(ct + i);
      }
      for (int i = 0; i < thread_num_; i++)
      {
        ct[i].wait();
        //tpool_.get_output();
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
  int64_t s0 = microseconds();
  WordSorter ws(thread_num, argv[2], argv[3]);
  int64_t s1 = microseconds();
  ws.write2output();
  int64_t s2 = microseconds();
  std::cout << "Loading and Sorting: " << s1 - s0 << "us\n";
  std::cout << "Writing: " << s2 - s1 << "us\n";
  std::cout << "Total elapsed time: " << s2 - s0 << "us" << std::endl;;
  return 0;
}

