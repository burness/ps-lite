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
class KVTableWorker : public SimpleApp {
public:
  using SimpleApp::obj_;
  using Callback = std::function<void()>;

  explicit KVTableWorker(int app_id, int customer_id) : SimpleApp() {

  }

  virtual ~KVTableWorker() {delete obj_; obj_ = nullptr;}

  int Pull(const std::vector<Key>& keys,
           std::vectorL<Val>* vals,
           std::vector<int>* lens = nullptr,)

private:

};

template <typename Val>
class KVTableServer : public SimpleApp {

};
}

#endif //PSLITE_KV_TABLE_APP_H
