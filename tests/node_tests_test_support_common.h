// Tests node worker admission, lifecycle hooks and exception propagation.
#pragma once
#include "pipe_test_support.h"
#include "takt/takt.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
