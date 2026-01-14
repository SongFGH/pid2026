###
 # @Description:
 # @Author: zhaoshanshan
 # @Date: 2023-04-19 14:30:42
###
set -o errexit

BASEDIR=../../data
QUERYDIR=${BASEDIR}/lu_querys_new/
PSTDIR=${BASEDIR}/lu_store_pst/
FLOWPST=${BASEDIR}/lu_store_flow_pst/
PSTCONF=${BASEDIR}/../conf/
REALDATADIR=${BASEDIR}/real_data/store_ad_pst/v2d3


day=$1
minutes=$2

lu_query_file_path=$(ls -lt ${QUERYDIR}/ | grep query_formal | head -n 1 |awk '{print $9}')
lu_pst_file_path=$(ls -lt ${PSTDIR}/ | grep pst_formal | head -n 1 |awk '{print $9}')
lu_flow_pst_file_path=$(ls -lt ${FLOWPST}/ | grep flow_pst_formal | head -n 1 |awk '{print $9}')
lu_pst_pid_info_path=$(ls -lt ${PSTCONF}/ | grep pst_formal | head -n 1 | awk '{print $9}')

echo ${QUERYDIR}/${lu_query_file_path}
echo ${PSTDIR}/${lu_pst_file_path}
echo ${FLOWPST}/${lu_flow_pst_file_path}
echo ${PSTCONF}/${lu_pst_pid_info_path}

time python ../../pysrc/v2d3/get_druid1_lu_real_data.py ${PSTDIR}/${lu_pst_file_path} ${REALDATADIR}/real_ab_data_${day}_${minutes}.txt real_ab_data ${QUERYDIR}/${lu_query_file_path} > ../logs/v2d3/real_ab_data_${day}_${minutes}.log

if [ ! -s ${REALDATADIR}/real_ab_data_${day}_${minutes}.txt ]; then
    rm ${REALDATADIR}/real_ab_data_${day}_${minutes}.txt
    echo "get real data fail"
fi
##不论本次实时数据是否获取成功，均根据最新一份实时数据进行调价
lu_real_data_file_path=$(ls -lt ${REALDATADIR}/ | grep real_ab_data | head -n 1 |awk '{print $9}')
echo ${REALDATADIR}/${lu_real_data_file_path}
#python ../pysrc/pid_control_v1.py ${day} ${QUERYDIR}/${lu_query_file_path} V0 >  logs/V0_${day}_${minutes}.log
#脚本v1和v3切换时，记得v3先获取v1的error

time python ../../pysrc/v2d3/pid_control_v6.py ${day} ${QUERYDIR}/${lu_query_file_path} V5 ${REALDATADIR}/${lu_real_data_file_path} ${PSTCONF}/${lu_pst_pid_info_path} ${FLOWPST}/${lu_flow_pst_file_path} >  ../logs/v2d3/V6_${day}_${minutes}.log


delete_day=`date -d "${day} -6 day " +%Y%m%d`
set +e
rm logs/*${delete_day}*
set -e

set +e
rm ${REALDATADIR}/real_ab_data_*${delete_day}*
set -e
