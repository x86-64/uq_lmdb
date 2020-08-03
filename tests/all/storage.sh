. tests/func.inc.sh

tmpStoragePath="$TMP_DIR"/test.storage.btree
tmpOutputPath="$TMP_DIR"/test.output

rm -rf $tmpStoragePath >/dev/null 2>&1 || true

$CMD -ct "$tmpStoragePath" < $TEST_ROOT/storage.data > $tmpOutputPath

md5=$(MD5 "$tmpOutputPath")
if [ "$md5" != 'fdc2ffc3edbab0ec2fc1a7678a33cb21' ]; then
	echo "Invalid checksum $md5"
	exit 1
fi
