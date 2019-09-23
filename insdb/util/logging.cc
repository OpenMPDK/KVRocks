/*
 * Original Code Copyright (c) 2011 The LevelDB Authors. All rights reserved.
 * Use of the original source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 *
 * Modifications made 2019
 * Modifications Copyright (c) 2019, Samsung Electronics.
 *
 * Architect    : Heekwon Park(heekwon.p@samsung.com), Yangseok Ki(yangseok.ki@samsung.com)
 * Authors      : Heekwon Park, Ilgu Hong, Hobin Lee
 *
 * This modified version is distributed under a BSD-style license that can be
 * found in the LICENSE.insdb file  
 *                    
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  
 */


#include "util/logging.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include "insdb/env.h"
#include "insdb/slice.h"

namespace insdb {

void AppendNumberTo(std::string* str, uint64_t num) {
  char buf[30];
  snprintf(buf, sizeof(buf), "%llu", (unsigned long long) num);
  str->append(buf);
}

void AppendEscapedStringTo(std::string* str, const Slice& value) {
  for (size_t i = 0; i < value.size(); i++) {
    char c = value[i];
    if (c >= ' ' && c <= '~') {
      str->push_back(c);
    } else {
      char buf[10];
      snprintf(buf, sizeof(buf), "\\x%02x",
               static_cast<unsigned int>(c) & 0xff);
      str->append(buf);
    }
  }
}

std::string NumberToString(uint64_t num) {
  std::string r;
  AppendNumberTo(&r, num);
  return r;
}

std::string EscapeString(const Slice& value) {
  std::string r;
  AppendEscapedStringTo(&r, value);
  return r;
}

bool ConsumeDecimalNumber(Slice* in, uint64_t* val) {
  uint64_t v = 0;
  int digits = 0;
  while (!in->empty()) {
    char c = (*in)[0];
    if (c >= '0' && c <= '9') {
      ++digits;
      // |delta| intentionally unit64_t to avoid Android crash (see log).
      const uint64_t delta = (c - '0');
      static const uint64_t kMaxUint64 = ~static_cast<uint64_t>(0);
      if (v > kMaxUint64/10 ||
          (v == kMaxUint64/10 && delta > kMaxUint64%10)) {
        // Overflow
        return false;
      }
      v = (v * 10) + delta;
      in->remove_prefix(1);
    } else {
      break;
    }
  }
  *val = v;
  return (digits > 0);
}

void dumpHex(void *id, const char *header, const char *buff, size_t size)
{
    if(header)
        printf("%p %s ", id, header);
    else
        printf("%p ", id);

    for(size_t i = 0; i < size; i++)
        printf("%02x ", (unsigned char)buff[i]);
    printf("\n");
}


}  // namespace insdb
