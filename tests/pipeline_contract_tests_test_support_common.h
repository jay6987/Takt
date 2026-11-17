// Verifies pipeline execution contracts with realistic source and sink nodes.
#pragma once
#include "pipe_test_support.h"
#include "takt/takt.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>
