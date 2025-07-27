#!/bin/bash

bin_path=`pwd`/build/bin

function clear_build_dir() {
  rm -rf build
  mkdir build
}

function build_src() {
  cd build
  cmake $1 ..
  make -j8
  cd -
}

function run_one() {
  printf "\n\e[36m\e[1mBegin run $bin_path/$1\e[0m\n"
  cd $bin_path
  printf "\e[35m\e[1mrun $bin_path/$1\e[0m\n"
  ./$1
  cd -
}

function run_all() {
  printf "\n\e[36m\e[1mBegin run all executables in $bin_path\e[0m\n"
  cd $bin_path
  for exe in $bin_path/*; do
      printf "\e[35m\e[1mrun $exe\e[0m\n"
      $exe
      printf "\n"
  done
  cd -
}

brand_new_build="false"
build_only="false"
build_with_san="non"
run_target="all"

while getopts "bos:e:" opt; do
  case $opt in
    b) brand_new_build="true";;
    o) build_only="true";;
    s) build_with_san=$OPTARG;;
    e) run_target=$OPTARG;;
    \?) exit 1;;
  esac
done

cmake_san_opt=""
if [ "$build_with_san" = "asan" ] || [ "$build_with_san" = "ASAN" ]; then
  cmake_san_opt="-DWITH_ASAN=ON"
  brand_new_build="true"
elif [ "$build_with_san" = "tsan" ] || [ "$build_with_san" = "TSAN" ]; then
  cmake_san_opt="-DWITH_TSAN=ON"
  brand_new_build="true"
elif [ "$build_with_san" = "ubsan" ] || [ "$build_with_san" = "UBSAN" ]; then
  cmake_san_opt="-DWITH_UBSAN=ON"
  brand_new_build="true"
fi

# build
if [ "$brand_new_build" = "true" ]; then
  printf "Brand new build\n"
  clear_build_dir
else
  printf "Not brand new build\n"
fi
build_src $cmake_san_opt

# no need to run
if [ "$build_only" = "true" ]; then
  exit 0
fi

# run
if [ "$run_target" = "all" ]; then
  run_all
else
  run_one $run_target
fi
