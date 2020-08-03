. tests/func.inc.sh

tmpStoragePath="$TMP_DIR"/uq_lmdb.format_seen.storage
tmpOutputPath="$TMP_DIR"/uq_lmdb.format_seen.output

rm -rf $tmpStoragePath >/dev/null 2>&1 || true

$CMD -t "$tmpStoragePath" -s < $TEST_ROOT/storage.data > $tmpOutputPath

md5=$(MD5 "$tmpOutputPath")
if [ "$md5" != 'b02aed2f556700f5fb865136c5fea6f7' ]; then
	echo "Invalid checksum $md5"
	exit 1
fi
