source env.sh
git clone --branch "${BRANCH}" "${REPO_URL}" repo
cd repo

./script/build.sh

./script/convert.sh ${INPUT_CSV} ${INPUT_SCHEMA} ${COLUMNAR}

for QUERY_NUM in {0..42}; do
  OUTPUT=${RESULTS}/query_${QUERY_NUM}.csv
  LOGS=${RESULTS}/query_${QUERY_NUM}.log
  ./script/run_query.sh ${QUERY_NUM} ${COLUMNAR} ${OUTPUT} ${LOGS}
done
