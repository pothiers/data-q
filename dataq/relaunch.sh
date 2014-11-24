# Source this after code modifications
killall -q -r dqsvc
pushd /sandbox/data-queue
sudo python3 setup.py install
cd /sandbox/tada
sudo python3 setup.py install
popd
source /sandbox/data-queue/dataq/launch.sh


