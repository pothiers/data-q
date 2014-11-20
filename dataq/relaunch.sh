# Source this after code modifications
killdq
pushd /sandbox/data-queue
sudo python3 setup.py install
cd /sandbox/tada
sudo python3 setup.py install
popd
source /sandbox/data-queue/dataq/launch.sh
dqcli --clear -s

