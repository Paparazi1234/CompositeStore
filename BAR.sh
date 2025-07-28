#!/bin/bash

build_dir=`pwd`/build
bin_dir=$build_dir/bin

function cleanup_build_dir() {
  rm -rf $build_dir
}

function clear_build_dir() {
  rm -rf $build_dir
  mkdir $build_dir
}

function build_src() {
  cd $build_dir
  cmake $1 -DBUILD_DIR="$build_dir" ..
  make -j8
  cd -
}

function run_one() {
  printf "\n\e[36m\e[1mBegin run $bin_dir/$1\e[0m\n"
  cd $bin_dir
  printf "\e[35m\e[1mrun $bin_dir/$1\e[0m\n"
  ./$1
  cd -
}

function run_all() {
  printf "\n\e[36m\e[1mBegin run all executables in $bin_dir\e[0m\n"
  cd $bin_dir
  for exe in $bin_dir/*; do
      printf "\e[35m\e[1mrun $exe\e[0m\n"
      $exe
      printf "\n"
  done
  cd -
}

cleanup_only="false"
brand_new_build="false"
build_only="false"
build_with_san="non"
run_target="all"

while getopts "cbos:e:" opt; do
  case $opt in
    c) cleanup_only="true";;
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

# cleanup build dir only
if [ "$cleanup_only" = "true" ]; then
  printf "Cleanup build dir only\n"
  cleanup_build_dir
  exit 0
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
