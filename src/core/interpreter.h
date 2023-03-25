// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <string_view>

#include "core/core_types.h"
#include "util/fibers/synchronization.h"

typedef struct lua_State lua_State;

namespace dfly {

class ObjectExplorer {
 public:
  virtual ~ObjectExplorer() {
  }

  virtual void OnBool(bool b) = 0;
  virtual void OnString(std::string_view str) = 0;
  virtual void OnDouble(double d) = 0;
  virtual void OnInt(int64_t val) = 0;
  virtual void OnArrayStart(unsigned len) = 0;
  virtual void OnArrayEnd() = 0;
  virtual void OnNil() = 0;
  virtual void OnStatus(std::string_view str) = 0;
  virtual void OnError(std::string_view str) = 0;
};

class Interpreter {
 public:
  using RedisFunc = std::function<void(MutSliceSpan, ObjectExplorer*)>;

  Interpreter();
  ~Interpreter();

  Interpreter(const Interpreter&) = delete;
  void operator=(const Interpreter&) = delete;

  Interpreter(Interpreter&&) = default;
  Interpreter& operator=(Interpreter&&) = default;

  // Note: We leak the state for now.
  // Production code should not access this method.
  lua_State* lua() {
    return lua_;
  }

  enum AddResult {
    ADD_OK = 0,
    ALREADY_EXISTS = 1,
    COMPILE_ERR = 2,
  };

  // Add function with sha and body to interpreter.
  AddResult AddFunction(std::string_view sha, std::string_view body, std::string* error);

  bool Exists(std::string_view sha) const;

  enum RunResult {
    RUN_OK = 0,
    NOT_EXISTS = 1,
    RUN_ERR = 2,
  };

  void SetGlobalArray(const char* name, MutSliceSpan args);

  // Runs already added function sha returned by a successful call to AddFunction().
  // Returns: true if the call succeeded, otherwise fills error and returns false.
  // sha must be 40 char length.
  RunResult RunFunction(std::string_view sha, std::string* err);

  // Checks whether the result is safe to serialize.
  // Should fit 2 conditions:
  // 1. Be the only value on the stack.
  // 2. Should have depth of no more than 128.
  bool IsResultSafe() const;

  void SerializeResult(ObjectExplorer* serializer);

  void ResetStack();

  // fp must point to buffer with at least 41 chars.
  // fp[40] will be set to '\0'.
  static void FuncSha1(std::string_view body, char* fp);

  template <typename U> void SetRedisFunc(U&& u) {
    redis_func_ = std::forward<U>(u);
  }

 private:
  // Returns true if function was successfully added,
  // otherwise returns false and sets the error.
  bool AddInternal(const char* f_id, std::string_view body, std::string* error);
  bool IsTableSafe() const;

  int RedisGenericCommand(bool raise_error);

  static int RedisCallCommand(lua_State* lua);
  static int RedisPCallCommand(lua_State* lua);

  lua_State* lua_;
  unsigned cmd_depth_ = 0;
  RedisFunc redis_func_;
};

// Manages an internal interpreter pool. This allows multiple connections residing on the same
// thread to run multiple lua scripts in parallel.
class InterpreterManager {
 public:
  InterpreterManager(unsigned num) : waker_{}, available_{}, storage_{} {
    // We pre-allocate the backing storage during initialization and
    // start storing pointers to slots in the available vector.
    storage_.reserve(num);
    available_.reserve(num);
  }

  // Borrow interpreter. Always return it after usage.
  Interpreter* Get();

  void Return(Interpreter*);

 private:
  ::util::fb2::EventCount waker_;
  std::vector<Interpreter*> available_;
  std::vector<Interpreter> storage_;
};

}  // namespace dfly
