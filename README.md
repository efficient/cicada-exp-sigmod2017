Cicada SIGMOD 2017 evaluation
=============================

Dependencies for experiment and analysis
----------------------------------------

 * bash >= 4.0
 * python >= 3.4
 * pandas >= 0.20
 * pandasql >= 0.7
 * matplotlib < 2.0

Compiling all engines
---------------------

         * ./build_cicada.sh
         * ./build_ermia.sh
         * ./build_foedus.sh
         * ./build_silo.sh

Running all experiments
-----------------------

         * run_exp.py exp_data_MYEXP run

Analyzing experiemnt results
----------------------------

 * Output files will be created in result_analysis/output_MYEXP

         * cd result_analysis
         * ./analyze.sh ../exp_data_MYEXP

Additional system configuration for third-party engines
-------------------------------------------------------

 * add to /etc/security/limits.conf: (replace [user] with the username)

        [user] - memlock unlimited

 * add to /etc/security/limits.conf: (replace [user] with the username)

        [user] - memlock unlimited
        [user] - nofile 655360
        [user] - nproc 655360
        [user] - rtprio 99

 * add to /etc/sysctl.conf: (run sudo sysctl -p -w; replace [HUGETLB_SHM_GROUP] with hugeshm's group ID)

        kernel.shmall=1152921504606846720
        kernel.shmmax=9223372036854775807
        kernel.shmmni=409600
        vm.max_map_count=2147483647
        vm.hugetlb_shm_group=HUGETLB_SHM_GROUP

Authors
-------

Hyeontaek Lim (hl@cs.cmu.edu)

License
-------

        Copyright 2014, 2015, 2016, 2017 Carnegie Mellon University

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing, software
        distributed under the License is distributed on an "AS IS" BASIS,
        WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        See the License for the specific language governing permissions and

