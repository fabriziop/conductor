cmake_minimum_required(VERSION 3.18)
project(conductor LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(pybind11 CONFIG REQUIRED)
find_package(Python3 COMPONENTS Interpreter REQUIRED)

pybind11_add_module(conductor src/conductor.cpp)

install(TARGETS conductor LIBRARY DESTINATION .)

option(CONDUCTOR_BUILD_EXAMPLES "Build C++ examples" ON)

if (CONDUCTOR_BUILD_EXAMPLES)
  add_custom_target(example_single_python_task DEPENDS conductor
    SOURCES examples/single_python_task.py)
  add_custom_target(example_multiple_task DEPENDS conductor
    SOURCES examples/multiple_task.py)
  add_custom_target(example_multi_pool_concurrent DEPENDS conductor
    SOURCES examples/multi_pool_concurrent.py)
  add_custom_target(example_one_shot_abs_time DEPENDS conductor
    SOURCES examples/one_shot_abs_time.py)
  add_custom_target(examples
    DEPENDS
      example_single_python_task
      example_multiple_task
      example_multi_pool_concurrent
      example_one_shot_abs_time)

  add_custom_target(run_example_single_python_task
    COMMAND ${CMAKE_COMMAND} -E env
      PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR}
      ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/examples/single_python_task.py
    DEPENDS conductor
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    USES_TERMINAL)

  add_custom_target(run_example_multiple_task
    COMMAND ${CMAKE_COMMAND} -E env
      PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR}
      ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/examples/multiple_task.py
    DEPENDS conductor
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    USES_TERMINAL)

  add_custom_target(run_example_multi_pool_concurrent
    COMMAND ${CMAKE_COMMAND} -E env
      PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR}
      ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/examples/multi_pool_concurrent.py
    DEPENDS conductor
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    USES_TERMINAL)

  add_custom_target(run_examples
    DEPENDS
      run_example_single_python_task
      run_example_multi_pool_concurrent)

  add_custom_target(run_regression_tests
    COMMAND ${CMAKE_COMMAND} -E env
      PYTHONPATH=${CMAKE_CURRENT_BINARY_DIR}
      ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/test_scheduler_regression.py
    DEPENDS conductor
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    USES_TERMINAL)
endif()

if (MSVC)
  target_compile_options(conductor PRIVATE /O2)
else()
  target_compile_options(conductor PRIVATE -O3)
endif()
