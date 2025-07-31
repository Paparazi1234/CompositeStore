#!/usr/bin/env bash

git grep -n 'namespace CompositeStore' -- '*.[ch]*'
if [ "$?" != "1" ]; then
  echo "^^^^^ Do not hardcode namespace CompositeStore. Use COMPOSITE_STORE_NAMESPACE"
  BAD=1
fi

git grep -n 'include <composite_store/' -- ':!build_tools/sanitize-sources.sh'
if [ "$?" != "1" ]; then
  echo '^^^^^ Use double-quotes as in #include "composite_store/something.h"'
  BAD=1
fi

git grep -n 'include "include/composite_store/' -- ':!build_tools/sanitize-sources.sh'
if [ "$?" != "1" ]; then
  echo '^^^^^ Use #include "composite_store/something.h" instead of #include "include/composite_store/something.h"'
  BAD=1
fi

git grep -n -P "[\x80-\xFF]" -- ':!docs' ':!*.md'
if [ "$?" != "1" ]; then
  echo '^^^^ Use only ASCII characters in source files'
  BAD=1
fi

if [ "$BAD" ]; then
  exit 1
fi
