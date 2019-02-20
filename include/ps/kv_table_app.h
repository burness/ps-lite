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
struct KVTablePairs {
    // keys 和vals应该有相同的size
    SArray<Key> keys;
    SArray<Val> vals;
};

template <typename Val>
class KVTableWorker : public SimpleApp {
public:
  using SimpleApp::obj_;
  using Callback = std::function<void()>;

  explicit KVTableWorker(int app_id, int customer_id) : SimpleApp() {
    using namespace std::placeholders;
    slicer_ = std::bind(&KVTableWorker<Val>::KVTableSlicer, this, _1, _2, _3);
    obj_ = new Customer(app_id, customer_id, std::bind(&KVTableWorker<Val>::Process, this, _1));

  }

  virtual ~KVTableWorker() {delete obj_; obj_ = nullptr;}

  int Pull(const std::vector<Key>& keys,
           const td::vectorL<Val>* vals,
           int cmd = 0,
           const Callback& cb = nullptr) {
    return ZPull(keys, vals, cmd, cb);
  }

  int ZPush(const SArray<Key>& keys,
            const SArray<Val>& vals,
            int cmd = 0,
            const Callback& cb = nullptr) {
    int ts = obj_->NewRequest(kServerGroup);
    KVTablePairs<Val> kvs;
    kvs.keys = keys;
    kvs.vals = vals;
    Send(ts, true, kvs, cmd, cb);
  }
  using SlicedKVs = std::vector<std::pair<bool, KVTablePairs<Val>>>;
  using Slicer = std::function<void(
          const KVTablePairs<Val>& send, const std::vector<Range>& ranges,
          SlicedKVs* sliced)>;

  int Pull(const std::vector<Key>& keys,
           std::vector<Val>* vals,
           int cmd = 0,
           const Callback& cb = nullptr) {
    int ts = Pull_(SArray<Key>(keys), vals, cmd, cb);
    TransformKVWorkerTable(keys, vals); // 转成hashMap来保存；
    return ts;
  }


  void Send(int timestamp, bool push, const KVTablePairs<Val>& kvs, int cmd, const Callback& cb = nullptr) {
    SlicedKVs sliced;
    slicer_(kvs, Postoffice::Get()->GetServerKeyRanges(), &sliced);

    int skipped = 0;
    for(size_t i = 0; i < sliced.size(); ++i) {
      if (!sliced[i].first) ++skipped;
    }
    obj_->AddResponse(timestamp, skipped);
    if ((size_t)skipped == sliced.size()) {
      RunCallback(timestamp);
    }

    for (size_t i = 0; i < sliced.size(); ++i) {
      const auto& s = sliced[i];
      if (!s.first) continue;
      Message msg;
      msg.meta.app_id = obj_->app_id();
      msg.meta.customer_id = obj_->customer_id();
      msg.meta.request = true;
      msg.meta.push = push;
      msg.meta.head = cmd;
      msg.meta.timestamp = timestamp;
      msg.meta.recver = Postoffice::Get()->ServerRankToID(i);
      const auto& kvs = s.second;
      if (kvs.keys.size()) {
        msg.AddData(kv.keys);
        msg.AddData(kv.vals);
      }
      Postoffice::Get()->van()->Send(msg);
    }
  }

  void KVTableSlicer(const KVTablePairs<Val>& send, const std::vector<Range>& ranges,
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
    CHECK_EQ(pos[n], send.keys.size());
    if (send.keys.empty()) return;

    size_t k = 0, val_begin = 0, val_end = 0;
    if (send.lens.empty()) {
      k = send.vals.size() / send.keys.size();
      CHECK_EQ(k * send.keys.size(), send.vals.size());
    } else {
      CHECK_EQ(send.keys.size(), send.lens.size());
    }

    for (size_t i = 0; i < n; ++i) {
      if (pos[i+1] == pos[i]) {
        sliced->at(i).first = false;
        continue;
      }
      sliced->at(i).first = true;
      auto& kv = sliced->at(i).second;
      kv.keys = send.keys.segment(pos[i], pos[i+1]);
      if (send.lens.size()) {
        kv.lens = send.lens.segment(pos[i], pos[i+1]);
        for(int l : kv.lens) val_end += l;
        kv.vals = send.vals.segment(val_begin, val_end);
        val_begin = val_end
      } else {
        kv.vals = send.vals.segment(pos[i]*k, pos[i+1]*k);
      }
    }
  }

  void Process(const Message& mgs) {
    if(msg.meta.simple_app) {
      SimpleApp::Process(msg); return;
    }

    int ts = msg.meta.timestamp;
    if (!msg.meta.push && msg.data.size()) {
      CHECK_GE(msg.data.size(), (size_t)2);
      KVTablePairs<Val> kvs;
      kvs.keys = msg.data[0];
      kvs.vals = msg.data[1];

      mu_.lock();
      recv_kvs_[ts].push_back(kvs);
      mu_.unlock();
    }

    if(obj_->NumResponse(ts) == Postoffice::Get()->num_servers() - 1) {
      RunCallback(ts);
    }
  }


private:
  Slicer slicer_;
  std::unordered_map<int, Callback> callbacks_;
  std::mutex mu_, kv_worker_table_mu_;
  std::unordered_map<int, std::vector<KVTablePairs<Val>>> recv_kvs_;
  std::unordered_map<Key, Val> kv_worker_table_;


  int Pull_(const SArray<Key>& keys, std::vector<Val>* vals, std::vector<int>* lens, int cmd, const Callback& cb) {
    int ts = obj_->NewRequest(kServerGroup);
    AddCallback(ts, [this, ts, keys, vals, lens, cb]() mutable {
      mu_.lock();
      auto& kvs = recv_kvs_[ts];
      mu_.unlock();

      size_t total_key = 0, total_val = 0;
      for(const auto& s : kvs) {
        Range range = FindRange(keys, s.keys.front(), s.keys.back()+1);
        CHECK_EQ(range.size(), s.keys.size()) << " unmatched keys size from one server";
        if (lens) CHECK_EQ(s.lens.size(), s.keys.size());
        total_key += s.keys.size();
        total_val += s.vals.size();
      }
      CHECK_EQ(total_key, keys.size()) << "lost some servers?";
      std::sort(kvs.begin(), kvs.end(), [](const KVTablePairs<Val>& a, const KVTablePairs<Val>& b) {
          return a.keys.front() < b.keys.front();
      });
      CHECK_NOTNULL(vals);
      if (vals->empty()) {
        vals->resize(total_val);
      } else {
        CHECK_EQ(vals->size(), total_val);
      }

      Val* ptr_vals = vals->data();

      for(const auto& s : kvs) {
        memcpy(p_vals, s.vals.data, s.vals.size() * sizeof(Val));
        p_vals += s.vals.size();
      }
      mu_.lock();
      recv_kvs_.erase(ts);
      mu_.unlock();
      if (cb) cb();
    })
    KVTablePairs<Val> kvs;
    kvs.keys = keys;
    Send(ts, false, cmd, kvs);
    return ts;
  }

  void TransformKVWorkerTable(const std::vector<Key>& keys,
                              const std::vector<Val>* vals) {
    CHECK_EQ(keys.size(), vals->size());
    kv_worker_table_mu_.lock();
    kv_worker_table_.clear();
    for(int i = 0; i < keys.size(); ++i) {
      kv_worker_table_[key] = vals->at(i);
    }
    kv_worker_table_mu_.unlock();
  }

  void AddCallback(int timestamp, const Callback& cb) {
    if (!cb) return;
    std::lock_guard<std::mutex> lk(mu_);
    callbacks_[timestamp] = cb;
  }

  void RunCallback(int timestamp) {
    mu_.lock();
    auto it = callbacks_.find(timestamp);
    if (it != callbacks_.end() {
      mu_.unlock();
      CHECK(it->second);
      it->second();
      mu_.lock();
      callbacks_.erase(it);
    }
    mu_.unlock();
  }


};

template <typename Val>
class KVTableServer : public SimpleApp {

};
}

#endif //PSLITE_KV_TABLE_APP_H
