Cicada SIGMOD 2017 evaluation
=============================

Hardware requirements
---------------------

 * Dual-socket Intel CPU >= Haswell
   * Interleaved CPU core ID mapping (even numbered cores on CPU 0, odd numbered cores on CPU 1)
   * Turbo Boost disabled for more accurate core scalability measurement
   * Hyperthreading enabled (though experiments do not use it)
 * DRAM >= 128 GiB
   * Ensure to use all memory channels for full memory bandwidth

Installing Base OS
-------

 * Ubuntu 14.04 LTS amd64 server

Installing packages
-------------------

	sudo apt-get update
	sudo apt-get install -y --force-yes software-properties-common
	sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test

	sudo apt-get update
	sudo apt-get install -y --force-yes cmake git g++-5 libnuma-dev make python3 python3-pip
	pip3 install --user 'pandas>=0.20,<0.21' 'pandasql>=0.7,<0.8' 'matplotlib>=1.5,<2.0'

 * Estimated time: 1 hour

Configuring system
------------------------------------------------

	# for non-interactive experiment execution
	echo "`whoami` ALL=(ALL:ALL) NOPASSWD:ALL" | sudo tee -a /etc/sudoers

	# for third-party engies
	echo "`whoami` - memlock unlimited" | sudo tee -a /etc/security/limits.conf
	echo "`whoami` - nofile 655360 | sudo tee -a /etc/security/limits.conf
	echo "`whoami` - nproc 655360 | sudo tee -a /etc/security/limits.conf
	echo "`whoami` - rtprio 99 | sudo tee -a /etc/security/limits.conf

	echo "kernel.shmall=1152921504606846720" | sudo tee -a /etc/sysctl.conf
	echo "kernel.shmmax=9223372036854775807" | sudo tee -a /etc/sysctl.conf
	echo "kernel.shmmni=409600" | sudo tee -a /etc/sysctl.conf
	echo "vm.max_map_count=2147483647" | sudo tee -a /etc/sysctl.conf
	echo "vm.hugetlb_shm_group=`grep '^hugeshm:' /etc/group | awk -F: -e '{print $3}'`" | sudo tee -a /etc/sysctl.conf

	sudo groupadd hugeshm

	sudo reboot

 * Estimated time: 15 minutes

Downloading source code
-----------------------

	git clone https://github.com/efficient/cicada-exp-sigmod2017.git
	cd cicada-exp-sigmod2017
	git submodule init
	git submodule update

 * Estimated time: 1 minute

Building all engines
--------------------

	./build_cicada.sh
	./build_ermia.sh
	./build_foedus.sh
	./build_silo.sh

 * Estimated time: 15 minutes

Running all experiments
-----------------------

	EXPNAME=MYEXP
	./run_exp.py exp_data_$EXPNAME run

 * Estimated time: 3 days
 * Experiment output files are created in exp\_data\_$EXPNAME
 * Each run automatically rebuilds DBx1000 (and cicada-engine for some experiments) to apply system/benchmark parameters

Analyzing experiment output
---------------------------

	cd result_analysis
	./analyze.sh ../exp_data_$EXPNAME

 * Estimated time: 15 minutes
 * Analysis output files are created under result\_analysis/output\_$EXPNAME

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

