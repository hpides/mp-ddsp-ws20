#!/bin/bash

find -regextype posix-extended -regex '.*(\.(h|cpp)|CMakeLists.txt)' -print0 | while IFS= read -r -d '' file;
do
  c=`tail -c 1 $file`
  echo $c
  if [ "$c" != "" ]; then
      echo "Missing newline at end of file: $file"
      exit 1;
  fi
done
