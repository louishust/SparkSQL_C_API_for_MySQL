project(test)
cmake_minimum_required(VERSION 2.8)
aux_source_directory(. SRC_LIST)
add_executable(${PROJECT_NAME} ${SRC_LIST})
include(UsePkgConfig)
pkgconfig(python includedir libdir linkflags cflags)
INCLUDE_DIRECTORIES(${includedir}) #-I
LINK_DIRECTORIES(${libdir}) #-L 
TARGET_LINK_LIBRARIES(${PROJECT_NAME} ${linkflags}) #-l 
ADD_DEFINITIONS(${cflags}) #-D