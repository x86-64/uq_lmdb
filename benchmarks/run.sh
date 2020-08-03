#!/bin/sh

testDir=$(pwd)/$(dirname $0)/all
rootDir=$(pwd)/$(dirname $0)/..

cd "$testDir"

export TEST_ROOT="$testDir"
export TMP_DIR="/tmp"
export CMD="./uq_lmdb"

echo "Running tests"

for test in *.sh; do

	echo " - $test ... "

	cd "$rootDir"

	sh "$testDir/$test" 2>&1

	cd "$testDir"
done

echo "done"
exit 0
