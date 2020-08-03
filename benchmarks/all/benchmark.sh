. tests/func.inc.sh

tmpStoragePath="$TMP_DIR"/uq_lmdb.benchmark.storage

rm -rf $tmpStoragePath >/dev/null 2>&1 || true

time seq 1 10000000 | $CMD -ct "$tmpStoragePath" > /dev/null

exit 0
