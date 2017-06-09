#!/bin/sh

../script/setup.sh 0 0

cd foedus_code || exit 1

[ ! -d build ] && mkdir build
cd build

cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo
make -j

echo "add to /etc/security/limits.conf: (replace [user] with the username)"
echo "[user] - memlock unlimited"
echo "[user] - nofile 655360"
echo "[user] - nproc 655360"
echo "[user] - rtprio 99"

echo "add to /etc/sysctl.conf: (run sudo sysctl -p -w; replace [HUGETLB_SHM_GROUP] with hugeshm's group ID)"
echo "kernel.shmall=1152921504606846720"
echo "kernel.shmmax=9223372036854775807"
echo "kernel.shmmni=409600"
echo "vm.max_map_count=2147483647"
echo "vm.hugetlb_shm_group=HUGETLB_SHM_GROUP"
