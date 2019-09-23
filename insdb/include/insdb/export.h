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


#ifndef STORAGE_INSDB_INCLUDE_EXPORT_H_
#define STORAGE_INSDB_INCLUDE_EXPORT_H_

#if !defined(INSDB_EXPORT)

#if defined(INSDB_SHARED_LIBRARY)
#if defined(OS_WIN)

#if defined(INSDB_COMPILE_LIBRARY)
#define INSDB_EXPORT __declspec(dllexport)
#else
#define INSDB_EXPORT __declspec(dllimport)
#endif  // defined(INSDB_COMPILE_LIBRARY)

#else  // defined(OS_WIN)
#if defined(INSDB_COMPILE_LIBRARY)
#define INSDB_EXPORT __attribute__((visibility("default")))
#else
#define INSDB_EXPORT
#endif
#endif  // defined(OS_WIN)

#else  // defined(INSDB_SHARED_LIBRARY)
#define INSDB_EXPORT
#endif

#endif  // !defined(INSDB_EXPORT)

#endif  // STORAGE_INSDB_INCLUDE_EXPORT_H_
