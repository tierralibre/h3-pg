#!/bin/bash
set -e

BASEDIR=$(dirname $(realpath "$0"))
REPOSITORY="docker.pkg.github.com/tierralibre/3RM"

cd $BASEDIR

function help {
  echo -e "Usage: $0"\\n
  echo -e "-b   --Build images"
  echo -e "-p   --Push images"
  echo -e "-t   --Run tests"
  echo -e "-u   --Run tests on i386"
}

while getopts 'hbptu' o; do
case "$o" in
	b)	docker build -t $REPOSITORY/test:amd64 --build-arg ARCH=amd64 .
      docker build -t $REPOSITORY/test:i386  --build-arg ARCH=i386  .
      exit 1;;
	p)	docker push $REPOSITORY/test:amd64
      docker push $REPOSITORY/test:i386
      exit 1;;
  t)  docker run --rm -v "$PWD"/../..:/github/workspace $REPOSITORY/test:amd64
      exit 1;;
  u)  docker run --rm -v "$PWD"/../..:/github/workspace $REPOSITORY/test:i386
      exit 1;;
	*) help;;
	esac
done

shift $((OPTIND-1))

if [ -z "${s}" ] || [ -z "${p}" ]; then
    help
fi
