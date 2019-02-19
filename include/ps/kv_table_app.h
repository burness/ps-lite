//
// Created by 段石石 on 2019/2/18.
//

#ifndef PSLITE_KV_TABLE_APP_H
#define PSLITE_KV_TABLE_APP_H
#include <algorithm>
#include <utility>
#include <vector>
#include "ps/base.h"
#include "ps/simple_app.h"

namespace ps {
template <typename Val>
struct KVPairs {
    SArray<Key> keys;
    SArray<Val> vals;
    SArray<int> lens;
};

template <typename Val>
class KVTableWorker : public SimpleApp {
public:
  using SimpleApp::obj_;
  using Callback = std::function<void()>;

  explicit KVTableWorker(int app_id, int customer_id) : SimpleApp() {
    using namespace std::placeholders;
    slicer_ = std::bind(&, this, _1, _2, _3);

  }

  virtual ~KVTableWorker() {delete obj_; obj_ = nullptr;}

  int Pull(const std::vector<Key>& keys,
           const td::vectorL<Val>* vals,
           const std::vector<int>* lens = nullptr
  )
    return ZPush(keys, vals, lens)

  int ZPush(const SArray<Key>& keys,
            const SArray<Val>& vals,
            const SArray<int>& lens = {}) {
    int ts = obj_->NewRequest(kServerGroup);
    KVPairs<Val> kvs;
    kvs.keys = keys;
    kvs.vals = vals;
    kvs.lens = lens;
    Send(ts, true, kvs);
  }
  using SlicedKVs = std::vector<std::pair<bool, KVPairs<Val>>>;
  using Slicer = std::function<void(
          const KVPairs<Val>& send, const std::vector<Range>& ranges,
          SlicedKVs* sliced)>;

  void Send(int timestamp, bool push , const KVPairs<Val>& kvs) {
    SlicedKVs sliced;
    slicer_(kvs, Postoffice::Get()->GetServerKeyRanges(), &sliced);
  }

  void DefaultSlicer(const KVPairs<Val>& send, const std::vector<Range>& ranges,
          typename SlicedKVs* sliced) {
    sliced->resize(ranges.size());

    size_t n = ranges.size();
    std::vector<size_t> pos(n+1);
    const Key* begin = send.keys.begin();
    const Key* end = send.keys.end();
    for (size_t i = 0; i < n; ++i) {
      if (i == 0) {
        pos[0] = std::lower_bound(begin, end, ranges[0].begin()) - begin;
        begin += pos[0];
      } else {
        CHECK_EQ(ranges[i-1].end(), ranges[i].begin());
      }
      size_t len = std::lower_bound(begin, end, ranges[i].end()) - begin;
      begin += len;
      pos[i+1] = pos[i] + len;

      sliced->at(i).first = (len != 0);
    }


  }



private:
    Slicer slicer_;

};

template <typename Val>
class KVTableServer : public SimpleApp {

};
}

#endif //PSLITE_KV_TABLE_APP_H
