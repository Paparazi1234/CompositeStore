#pragma once

#include <vector>
#include <tuple>
#include <assert.h>

#include "include/composite_store_namespace.h"

namespace COMPOSITE_STORE_NAMESPACE {

class IteratorCursor {
 public:
  IteratorCursor() {}
  IteratorCursor(const std::vector<size_t>& cursors_info) {
    Initialize(cursors_info);
  }

  void Initialize(const std::vector<size_t>& cursors_info) {
    cursors_.resize(cursors_info.size());
    for (uint32_t i = 0; i < cursors_info.size(); ++i) {
      cursors_[i].curr = 0;
      cursors_[i].count = cursors_info[i];
    }
  }

  void SeekToFirst() {
    for (uint32_t i = 0; i < cursors_.size(); ++i) {
      cursors_[i].curr = 0;
    }
  }

  void Next() {
    if (!Valid()) {
      return;
    }

    int32_t i = cursors_.size() - 1;
    while (i >= 0 && Advance(i)) {
      --i;
    }
  }

  bool Valid() const {
    return cursors_[0].curr != cursors_[0].count;
  }

  size_t CursorAt(size_t i) const {
    return cursors_[i].curr;
  }

 private:
  struct Cursor {
    size_t curr;
    size_t count;
  };

  bool Advance(int32_t i) {
    assert(i >= 0);
    cursors_[i].curr += 1;
    // when cursors_[0].curr == cursors_[0].count, it means we have yielded
    // all results
    if (i != 0 && cursors_[i].curr == cursors_[i].count) {
      cursors_[i].curr = 0;
      return true;
    }
    return false;
  }

  std::vector<Cursor> cursors_;
};

template <typename T0, typename T1>
class CartesianProductGenerator2 {
 public:
  CartesianProductGenerator2(const std::vector<T0>& vec_t0,
                             const std::vector<T1>& vec_t1) {
    vec_t0_.resize(vec_t0.size());
    vec_t1_.resize(vec_t1.size());
    vec_t0_ = vec_t0;
    vec_t1_ = vec_t1;
  }

  class Iterator {
   public:
    Iterator(const CartesianProductGenerator2<T0, T1>* generator)
        : generator_(generator) {
      std::vector<size_t> cursors_info =
          {generator_->vec_t0_.size(), generator_->vec_t1_.size()};
      cursor_.Initialize(cursors_info);
    }
    void SeekToFirst() { cursor_.SeekToFirst(); }
    void Next() { cursor_.Next(); }
    bool Valid() const { return cursor_.Valid(); }

    void Value(T0* t0, T1* t1) const {
      assert(Valid());
      *t0 = generator_->vec_t0_[cursor_.CursorAt(0)];
      *t1 = generator_->vec_t1_[cursor_.CursorAt(1)];
    }

    std::tuple<T0, T1> Value() const {
      assert(Valid());
      return std::make_tuple(generator_->vec_t0_[cursor_.CursorAt(0)],
                             generator_->vec_t1_[cursor_.CursorAt(1)]);
    }
   private:
    const CartesianProductGenerator2<T0, T1>* generator_;
    IteratorCursor cursor_;
  };

 private:
  std::vector<T0> vec_t0_;
  std::vector<T1> vec_t1_;
};

template <typename T0, typename T1, typename T2>
class CartesianProductGenerator3 {
 public:
  CartesianProductGenerator3(const std::vector<T0>& vec_t0,
                             const std::vector<T1>& vec_t1,
                             const std::vector<T2>& vec_t2) {
    vec_t0_.resize(vec_t0.size());
    vec_t1_.resize(vec_t1.size());
    vec_t2_.resize(vec_t2.size());
    vec_t0_ = vec_t0;
    vec_t1_ = vec_t1;
    vec_t2_ = vec_t2;
  }

  class Iterator {
   public:
    Iterator(const CartesianProductGenerator3<T0, T1, T2>* generator)
        : generator_(generator) {
      std::vector<size_t> cursors_info =
          {generator_->vec_t0_.size(), generator_->vec_t1_.size(),
           generator_->vec_t2_.size()};
      cursor_.Initialize(cursors_info);
    }
    void SeekToFirst() { cursor_.SeekToFirst(); }
    void Next() { cursor_.Next(); }
    bool Valid() const { return cursor_.Valid(); }

    void Value(T0* t0, T1* t1, T2* t2) const {
      assert(Valid());
      *t0 = generator_->vec_t0_[cursor_.CursorAt(0)];
      *t1 = generator_->vec_t1_[cursor_.CursorAt(1)];
      *t2 = generator_->vec_t2_[cursor_.CursorAt(2)];
    }

    std::tuple<T0, T1, T2> Value() const {
      assert(Valid());
      return std::make_tuple(generator_->vec_t0_[cursor_.CursorAt(0)],
                             generator_->vec_t1_[cursor_.CursorAt(1)],
                             generator_->vec_t2_[cursor_.CursorAt(2)]);
    }
   private:
    const CartesianProductGenerator3<T0, T1, T2>* generator_;
    IteratorCursor cursor_;
  };

 private:
  std::vector<T0> vec_t0_;
  std::vector<T1> vec_t1_;
  std::vector<T2> vec_t2_;
};

template <typename T0, typename T1, typename T2, typename T3>
class CartesianProductGenerator4 {
 public:
  CartesianProductGenerator4(const std::vector<T0>& vec_t0,
                             const std::vector<T1>& vec_t1,
                             const std::vector<T2>& vec_t2,
                             const std::vector<T3>& vec_t3) {
    vec_t0_.resize(vec_t0.size());
    vec_t1_.resize(vec_t1.size());
    vec_t2_.resize(vec_t2.size());
    vec_t3_.resize(vec_t3.size());
    vec_t0_ = vec_t0;
    vec_t1_ = vec_t1;
    vec_t2_ = vec_t2;
    vec_t3_ = vec_t3;
  }

  class Iterator {
   public:
    Iterator(const CartesianProductGenerator4<T0, T1, T2, T3>* generator)
        : generator_(generator) {
      std::vector<size_t> cursors_info =
          {generator_->vec_t0_.size(), generator_->vec_t1_.size(),
           generator_->vec_t2_.size(), generator_->vec_t3_.size()};
      cursor_.Initialize(cursors_info);
    }
    void SeekToFirst() { cursor_.SeekToFirst(); }
    void Next() { cursor_.Next(); }
    bool Valid() const { return cursor_.Valid(); }

    void Value(T0* t0, T1* t1, T2* t2, T3* t3) const {
      assert(Valid());
      *t0 = generator_->vec_t0_[cursor_.CursorAt(0)];
      *t1 = generator_->vec_t1_[cursor_.CursorAt(1)];
      *t2 = generator_->vec_t2_[cursor_.CursorAt(2)];
      *t3 = generator_->vec_t3_[cursor_.CursorAt(3)];
    }

    std::tuple<T0, T1, T2, T3> Value() const {
      assert(Valid());
      return std::make_tuple(generator_->vec_t0_[cursor_.CursorAt(0)],
                             generator_->vec_t1_[cursor_.CursorAt(1)],
                             generator_->vec_t2_[cursor_.CursorAt(2)],
                             generator_->vec_t3_[cursor_.CursorAt(3)]);
    }
   private:
    const CartesianProductGenerator4<T0, T1, T2, T3>* generator_;
    IteratorCursor cursor_;
  };

 private:
  std::vector<T0> vec_t0_;
  std::vector<T1> vec_t1_;
  std::vector<T2> vec_t2_;
  std::vector<T3> vec_t3_;
};

template <typename T0, typename T1, typename T2, typename T3, typename T4>
class CartesianProductGenerator5 {
 public:
  CartesianProductGenerator5(const std::vector<T0>& vec_t0,
                             const std::vector<T1>& vec_t1,
                             const std::vector<T2>& vec_t2,
                             const std::vector<T3>& vec_t3,
                             const std::vector<T4>& vec_t4) {
    vec_t0_.resize(vec_t0.size());
    vec_t1_.resize(vec_t1.size());
    vec_t2_.resize(vec_t2.size());
    vec_t3_.resize(vec_t3.size());
    vec_t4_.resize(vec_t4.size());
    vec_t0_ = vec_t0;
    vec_t1_ = vec_t1;
    vec_t2_ = vec_t2;
    vec_t3_ = vec_t3;
    vec_t4_ = vec_t4;
  }

  class Iterator {
   public:
    Iterator(const CartesianProductGenerator5<T0, T1, T2, T3, T4>* generator)
        : generator_(generator) {
      std::vector<size_t> cursors_info =
          {generator_->vec_t0_.size(), generator_->vec_t1_.size(),
           generator_->vec_t2_.size(), generator_->vec_t3_.size(),
           generator_->vec_t4_.size()};
      cursor_.Initialize(cursors_info);
    }
    void SeekToFirst() { cursor_.SeekToFirst(); }
    void Next() { cursor_.Next(); }
    bool Valid() const { return cursor_.Valid(); }

    void Value(T0* t0, T1* t1, T2* t2, T3* t3, T4* t4) const {
      assert(Valid());
      *t0 = generator_->vec_t0_[cursor_.CursorAt(0)];
      *t1 = generator_->vec_t1_[cursor_.CursorAt(1)];
      *t2 = generator_->vec_t2_[cursor_.CursorAt(2)];
      *t3 = generator_->vec_t3_[cursor_.CursorAt(3)];
      *t4 = generator_->vec_t4_[cursor_.CursorAt(4)];
    }

    std::tuple<T0, T1, T2, T3, T4> Value() const {
      assert(Valid());
      return std::make_tuple(generator_->vec_t0_[cursor_.CursorAt(0)],
                             generator_->vec_t1_[cursor_.CursorAt(1)],
                             generator_->vec_t2_[cursor_.CursorAt(2)],
                             generator_->vec_t3_[cursor_.CursorAt(3)],
                             generator_->vec_t4_[cursor_.CursorAt(4)]);
    }
   private:
    const CartesianProductGenerator5<T0, T1, T2, T3, T4>* generator_;
    IteratorCursor cursor_;
  };

 private:
  std::vector<T0> vec_t0_;
  std::vector<T1> vec_t1_;
  std::vector<T2> vec_t2_;
  std::vector<T3> vec_t3_;
  std::vector<T4> vec_t4_;
};

}   // namespace COMPOSITE_STORE_NAMESPACE
