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

#include "thread.h"
#include "thread_store.h"

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


class WordSorter
{
  public:
    class Word
    {
      public:
        Word() : //w_(0)
          w1_(0), w2_(0)
        {}
        Word(const char * ptr, const int64_t len)
            : //w_(0)
              w1_(0), w2_(0)
        {
          len_ = len;
          int64_t m = std::min(len, 8L);
          for (int64_t i = 0; i < m; i++)
          {
            w1_ |= static_cast<int64_t>((ptr[i] - 'a')) << ((7 - i) * 5);
          }
          if (len > 8)
          {
            for (int64_t i = 8; i < len; i++)
            {
              w2_ |= static_cast<int64_t>((ptr[i] - 'a')) << ((15 - i) * 5);
            }
          }
          //printf("%ld %ld\n", w1_, w2_);
        }

        Word(const Word & r) : //w_(r.w_)
          w1_(r.w1_), w2_(r.w2_), len_(r.len_)
        {}

        Word & operator = (const Word & r)
        {
          //w_ = r.w_;
          w1_ = r.w1_;
          w2_ = r.w2_;
          len_ = r.len_;
          return *this;
        }

        bool operator < (const Word & r) const
        {
          //return w_ < r.w_;
          return w1_ < r.w1_ || (w1_ == r.w1_ && w2_ < r.w2_);
        }

        const char* get_ptr() const
        {
          std::string * p = output_buf.get();
          if (p != NULL)
          {
            p->clear();

            int64_t m = std::min(len_, 8L);
            for (int64_t i = 0; i < m; i++)
            {
              p->push_back(static_cast<char>((w1_ >> ((7 - i) * 5)) & 0x1F) + 'a');
            }
            if (len_ > 8)
            {
              for (int64_t i = 0; i < len_ - 8; i++)
              {
                p->push_back(static_cast<char>((w2_ >> ((7 - i) * 5)) & 0x1F) + 'a');
              }
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
        }

      protected:
        //__int128 w_;
        int64_t w1_;
        int64_t w2_;
        int64_t len_;
    };

    static thread_store<std::string> output_buf;

    typedef std::vector<Word>   TWordList;
    typedef TWordList::iterator TWordListIter;
  public:
    WordSorter(const std::string & input_file, const std::string & output_file)
      : input_file_(input_file), output_file_(output_file),
        fd_(-1), buf_(NULL), buf_len_(0), file_size_(0)
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

      split_words();
    }

    void split_words()
    {
      int64_t s0 = microseconds();
      int64_t start = 0;
      for (int64_t i = 0; i < buf_len_; i++)
      {
        if (isspace(buf_[i]))
        {
          if (i > start)
          {
            list_.get_cur_list().push_back(Word(buf_ + start, i - start));
          }
          start = i + 1;
        }
      }
      if (start < buf_len_)
      {
        list_.get_cur_list().push_back(Word(buf_ + start, buf_len_ - start));
      }
      std::cout << "Total words: " << list_.get_cur_list().size() << std::endl;
      int64_t s1 = microseconds();
      std::cout << "splitting used: " << s1 - s0 << std::endl;
      //for (int i = 0; i < 9; i++)
      //{
      //  printf("len = %ld word = %s\n", list_.get_cur_list()[i].get_len(),
      //      list_.get_cur_list()[i].get_ptr());
      //}
    }

    static bool sort_comp(const Word & l, const Word & r)
    {
      return l < r;
    }

    void sort()
    {
      std::sort(list_.get_cur_list().begin(), list_.get_cur_list().end(), sort_comp);
    }

    class SortThread : public Thread
    {
      public:
        void * run(void * arg)
        {
          MergeParam * param = reinterpret_cast<MergeParam *>(arg);
          std::sort(param->begin, param->end, sort_comp);
        }
    };

    static void print_words(TWordListIter begin, TWordListIter end)
    {
      for (TWordListIter iter = begin; iter != end; iter++)
      {
        iter->print();
        std::cout << " | " ;
      }
      std::cout << std::endl;
    }

    class Acceptor
    {
      public:
        virtual ~Acceptor() {}
        virtual void accept(Word & word) = 0;
    };

    class ListAcceptor : public Acceptor
    {
      public:
        ListAcceptor(TWordListIter & begin) : cur_(begin) {}
        void accept(Word & word)
        {
          *cur_++ = word;
        }
      protected:
        TWordListIter & cur_;
    };

    class FileAcceptor : public Acceptor
    {
      public:
        FileAcceptor(const std::string & filename, int64_t file_size) : fd_(-1), buf_index_(0)
        {
          fd_ = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
          if (fd_ == -1)
          {
            throw new std::exception();
          }
          buf_ = static_cast<char *>(malloc(file_size));
          if (NULL == buf_)
          {
            throw new std::bad_alloc();
          }
        }
        ~FileAcceptor()
        {
          if (-1 == write(fd_, buf_, buf_index_))
          {
            abort();
          }
          close(fd_);
        }
        void accept(Word & word)
        {
          memcpy(buf_ + buf_index_, word.get_ptr(), word.get_len());
          buf_index_ += word.get_len();
          buf_[buf_index_] = '\n';
          buf_index_ ++; 
        }
      protected:
        int fd_;
        char * buf_;
        int64_t buf_index_;
    };


    struct MergeParam
    {
      TWordListIter begin;
      TWordListIter end;
      TWordListIter begin2;
      TWordListIter end2;
      TWordListIter dest;
    };
    typedef std::vector<MergeParam> TMergeParams;
    typedef TMergeParams::iterator TMergeParamsIter;

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

        TMergeParams & get_cur_param()
        {
          return merge_params_[cur_index_];
        }

        TMergeParams & get_2nd_param()
        {
          return merge_params_[1 - cur_index_];
        }

        void switch_index()
        {
          cur_index_ = 1 - cur_index_;
        }

      protected:
        TWordList word_list_[2];
        TMergeParams merge_params_[2];
        int cur_index_;
    };

    class Merger
    {
      public:
        void add_way(const MergeParam & param)
        {
          params_.push_back(param);
        }
        void add_way(const TMergeParamsIter & begin,
            const TMergeParamsIter & end)
        {
          for (TMergeParamsIter iter = begin; iter != end; iter++)
          {
            add_way(*iter);
          }
        }
        static bool merge_comp(const MergeParam & l, const MergeParam & r)
        {
          if (l.begin >= l.end) return true;
          if (r.begin >= r.end) return false;
          return !(*l.begin < *r.begin);
        }
        void merge(Acceptor & acceptor)
        {
          if (params_.size() == 1)
          {
            for (TWordListIter iter = params_[0].begin;
                iter != params_[0].end; iter++)
            {
              acceptor.accept(*iter);
            }
          }
          else if (params_.size() == 2)
          {
            int64_t total_len = (params_[0].end - params_[0].begin)
              + (params_[1].end - params_[1].begin);
            for (int64_t i = 0; i < total_len; i++)
            {
              if (params_[0].begin != params_[0].end)
              {
                if (params_[1].begin != params_[1].end)
                {
                  if ((*params_[0].begin) < (*params_[1].begin))
                  {
                    acceptor.accept(*params_[0].begin++);
                  }
                  else
                  {
                    acceptor.accept(*params_[1].begin++);
                  }
                }
                else
                {
                  acceptor.accept(*params_[0].begin++);
                }
              }
              else
              {
                if (params_[1].begin != params_[1].end)
                {
                  acceptor.accept(*params_[1].begin++);
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
            make_heap(params_.begin(), params_.end(), merge_comp);
            while (!is_end())
            {
              acceptor.accept(*params_[0].begin);
              params_[0].begin++;
              make_heap(params_.begin(), params_.end(), merge_comp);
            }
          }
        }
        bool is_end() const
        {
          return params_[0].begin >= params_[0].end;
        }
      protected:
        TMergeParams params_;
    };

    class MergeThread : public Thread
    {
      public:
        void * run(void * arg)
        {
          MergeParam * param = reinterpret_cast<MergeParam *>(arg);
          //print_words(param->begin, param->end);
          //print_words(param->begin2, param->end2);
          MergeParam param2;
          param2.begin = param->begin2;
          param2.end = param->end2;
          Merger merger;
          merger.add_way(*param);
          merger.add_way(param2);
          param->begin = param->dest;
          ListAcceptor acceptor(param->dest);
          merger.merge(acceptor);
          param->end = param->dest;
        }
    };

    void threaded_sort(int64_t thread_num)
    {
      SortThread * t = new SortThread[thread_num];
      list_.get_cur_param().resize(thread_num);
      int64_t word_per_thread = list_.get_cur_list().size() / thread_num;
      for (int64_t i = 0; i < thread_num - 1; i++)
      {
        list_.get_cur_param()[i].begin = list_.get_cur_list().begin() + i * word_per_thread;
        list_.get_cur_param()[i].end = list_.get_cur_list().begin() + (i + 1) * word_per_thread;
      }
      list_.get_cur_param()[thread_num - 1].begin = list_.get_cur_list().begin() + (thread_num - 1) * word_per_thread;
      list_.get_cur_param()[thread_num - 1].end = list_.get_cur_list().end();
      for (int64_t i = 0; i < thread_num; i++)
      {
        t[i].start(&(list_.get_cur_param()[i]));
      }
      for (int64_t i = 0; i < thread_num; i++)
      {
        t[i].wait();
      }

      int64_t split_num = thread_num;
      list_.get_2nd_list().resize(list_.get_cur_list().size());
      list_.get_2nd_param().resize(split_num);
      MergeThread * m = new MergeThread[split_num];
      //print_words(list_.get_cur_list().begin(), list_.get_cur_list().end());
      while (split_num > 1)
      {
        int64_t merge_num = split_num / 2;
        for (int64_t i = 0; i < merge_num; i++)
        {
          list_.get_2nd_param()[i].begin  = list_.get_cur_param()[i * 2].begin;
          list_.get_2nd_param()[i].end    = list_.get_cur_param()[i * 2].end;
          list_.get_2nd_param()[i].begin2 = list_.get_cur_param()[i * 2 + 1].begin;
          list_.get_2nd_param()[i].end2   = list_.get_cur_param()[i * 2 + 1].end;
          int64_t pos = list_.get_cur_param()[i * 2].begin - list_.get_cur_list().begin();
          //std::cout << "pos = " << pos << std::endl;
          list_.get_2nd_param()[i].dest   = list_.get_2nd_list().begin() + pos;
          m[i].start(&(list_.get_2nd_param()[i]));
          //m[i].wait();
          //std::cout << "begin = " << list_.get_2nd_param()[i].begin - list_.get_2nd_list().begin()
          //  << " , end = " << list_.get_2nd_param()[i].end - list_.get_2nd_list().begin()
          //  << std::endl;
        }
        if (split_num % 2 == 1)
        {
          list_.get_2nd_param()[merge_num].begin = list_.get_cur_param()[split_num - 1].begin;
          list_.get_2nd_param()[merge_num].end   = list_.get_cur_param()[split_num - 1].end;
          split_num = merge_num + 1;
          Merger merger;
          merger.add_way(list_.get_2nd_param()[merge_num]);
          TWordListIter ac = list_.get_2nd_list().begin()
              + (list_.get_cur_param()[merge_num].begin - list_.get_cur_list().begin());
          list_.get_2nd_param()[merge_num].begin = ac;
          ListAcceptor acceptor(ac);
          merger.merge(acceptor);
          list_.get_2nd_param()[merge_num].end = ac;
        }
        else
        {
          split_num = merge_num;
        }
        for (int64_t i = 0; i < merge_num; i++)
        {
          m[i].wait();
        }
        list_.switch_index();
        //print_words(list_.get_cur_list().begin(), list_.get_cur_list().end());
      }
      //print_words(list_.get_cur_list().begin(), list_.get_cur_list().end());
    }

    class ConcatThread : public Thread
    {
      public:
        void * run(void * arg)
        {
          for (TWordListIter iter = begin_; iter != end_; iter++)
          {
            memcpy(buf_ + len_, iter->get_ptr(), iter->get_len());
            len_ += iter->get_len();
            buf_[len_] = '\n';
            len_ ++; 
          }
        }

        void init(TWordListIter begin, TWordListIter end, int64_t file_size)
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

      protected:
        TWordListIter begin_, end_;
        char * buf_;
        int64_t len_;
        int64_t file_size_;
    };

    void write2output()
    {
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
      //for (TWordListIter iter = list_.get_cur_list().begin();
      //    iter != list_.get_cur_list().end(); iter++)
      //{
      //  memcpy(obuf + obufi, (*iter)->get_ptr(), (*iter)->get_len());
      //  obufi += (*iter)->get_len();
      //  obuf[obufi] = '\n';
      //  obufi ++; 
      //}

      const int THREAD_NUM = sysconf(_SC_NPROCESSORS_ONLN);
      int64_t num_per_thread = list_.get_cur_list().size() / THREAD_NUM;
      ConcatThread * ct = new ConcatThread[THREAD_NUM];
      for (int i = 0; i < THREAD_NUM; i++)
      {
        TWordListIter begin = list_.get_cur_list().begin() + i * num_per_thread;
        TWordListIter end = list_.get_cur_list().begin() + (i + 1) * num_per_thread;
        if (i == THREAD_NUM - 1)
        {
          end = list_.get_cur_list().end();
        }
        ct[i].init(begin, end, file_size_);
        ct[i].start();
      }
      for (int i = 0; i < THREAD_NUM; i++)
      {
        ct[i].wait();
      }
      int64_t s1 = microseconds();
      std::cout << "concating used: " << s1 - s0 << std::endl;

      for (int i = 0; i < THREAD_NUM; i++)
      {
        int size = write(ofd, ct[i].get_buf(), ct[i].get_len());
        if (size == -1)
        {
          throw new std::exception();
        }
      }

      close(ofd);
    }

    void threaded_write2output()
    {
      FileAcceptor acceptor(output_file_, file_size_);
      Merger merger;
      merger.add_way(list_.get_cur_param().begin(), list_.get_cur_param().end());
      merger.merge(acceptor);
    }

  protected:
    std::string input_file_;
    std::string output_file_;
    int fd_;
    char * buf_;
    int64_t buf_len_;
    int64_t file_size_;
    //TWordList word_list_;
    //TMergeParams merge_params_;
    DWordList list_;
};

thread_store<std::string> WordSorter::output_buf;

int main(int argc, char * argv[])
{
  if (argc < 4)
  {
    std::cerr << "\nusage: " << argv[0]
      << " <thread number> <input file> <output file>\n" << std::endl;;
    exit(1);
  }
  int thread_num = atoi(argv[1]);
  int64_t s0 = microseconds();
  WordSorter ws(argv[2], argv[3]);
  int64_t s1 = microseconds();
  //ws.sort();
  ws.threaded_sort(thread_num);
  int64_t s2 = microseconds();
  ws.write2output();
  //ws.threaded_write2output();
  int64_t s3 = microseconds();
  std::cout << "Loading: " << s1 - s0 << "us\n";
  std::cout << "Sorting: " << s2 - s1 << "us\n";
  std::cout << "Writing: " << s3 - s2 << "us\n";
  std::cout << "Total elapsed time: " << s3 - s0 << "us" << std::endl;;
  return 0;
}

