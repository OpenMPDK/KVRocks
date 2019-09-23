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


#include "insdb/env.h"
#include "port/port.h"
#include "util/coding.h"
namespace insdb {

InSDBKey EncodeInSDBKey(InSDBKey key) {
    if (port::kLittleEndian) return key;
    InSDBKey new_key;
    EncodeFixed64((char*)&new_key.first, key.first);
    EncodeFixed64((char*)&new_key.second, key.second);
    return new_key;
}

Env::~Env() {
}


Logger::~Logger() {
}


void Log(Logger* info_log, const char* format, ...) {
  if (info_log != NULL) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(format, ap);
    va_end(ap);
  }
}

EnvWrapper::~EnvWrapper() {
}

}  // namespace insdb
