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


// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#ifndef STORAGE_INSDB_INCLUDE_STATUS_H_
#define STORAGE_INSDB_INCLUDE_STATUS_H_

#include <string>
#include "insdb/export.h"
#include "insdb/slice.h"

namespace insdb {

    class INSDB_EXPORT Status {
        public:
            // Create a success status.
            Status() : state_(NULL) { }
            ~Status() { delete[] state_; }

            // Copy the specified status.
            Status(const Status& s);
            void operator=(const Status& s);

            // Return a success status.
            static Status OK() { return Status(); }

            // Return error status of an appropriate type.
            static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
                return Status(kNotFound, msg, msg2);
            }
            static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
                return Status(kCorruption, msg, msg2);
            }
            static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
                return Status(kNotSupported, msg, msg2);
            }
            static Status NotAvailable(const Slice& msg, const Slice& msg2 = Slice()) {
                return Status(kNotAvailable, msg, msg2);
            }
            static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
                return Status(kInvalidArgument, msg, msg2);
            }
            static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
                return Status(kIOError, msg, msg2);
            }

            // Returns true iff the status indicates success.
            bool ok() const { return (state_ == NULL); }

            // Returns true iff the status indicates a NotFound error.
            bool IsNotFound() const { return code() == kNotFound; }

            // Returns true iff the status indicates a Corruption error.
            bool IsCorruption() const { return code() == kCorruption; }

            // Returns true iff the status indicates an IOError.
            bool IsIOError() const { return code() == kIOError; }

            // Returns true iff the status indicates a NotSupportedError.
            bool IsNotSupportedError() const { return code() == kNotSupported; }

            // Returns true iff the status indicates a NotSupportedError.
            bool IsNotAvailable() const { return code() == kNotAvailable; }

            // Returns true iff the status indicates an InvalidArgument.
            bool IsInvalidArgument() const { return code() == kInvalidArgument; }

            // Return a string representation of this status suitable for printing.
            // Returns the string "OK" for success.
            std::string ToString() const;

        private:
            // OK status has a NULL state_.  Otherwise, state_ is a new[] array
            // of the following form:
            //    state_[0..3] == length of message
            //    state_[4]    == code
            //    state_[5..]  == message
            const char* state_;

            enum Code {
                kOk = 0,
                kNotFound = 1,
                kCorruption = 2,
                kNotSupported = 3,
                kNotAvailable = 4,
                kInvalidArgument = 5,
                kIOError = 6
            };

            Code code() const {
                return (state_ == NULL) ? kOk : static_cast<Code>(state_[4]);
            }

            Status(Code code, const Slice& msg, const Slice& msg2);
            static const char* CopyState(const char* s);
    };

    inline Status::Status(const Status& s) {
        state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
    }
    inline void Status::operator=(const Status& s) {
        // The following condition catches both aliasing (when this == &s),
        // and the common case where both s and *this are ok.
        if (state_ != s.state_) {
            delete[] state_;
            state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
        }
    }

}  // namespace insdb

#endif  // STORAGE_INSDB_INCLUDE_STATUS_H_
