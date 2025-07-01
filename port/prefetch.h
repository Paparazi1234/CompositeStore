//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#if defined(__aarch64__)
//  __builtin_prefetch(..., 1) turns into a prefetch into prfm pldl3keep. On
// arm64 we want this as close to the core as possible to turn it into a
// L1 prefetech unless locality == 0 in which case it will be turned into a
// non-temporal prefetch
#define PREFETCH(addr, rw, locality) \
  __builtin_prefetch(addr, rw, locality >= 1 ? 3 : locality)
#else
#define PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)
#endif