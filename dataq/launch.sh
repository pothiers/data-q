# SOURCE this
# This is SCAFFOLDING only.  It should be removed when provisioning in place!!!

export LOGDIR=/var/log/tada


#!if [ "`hostname`"  = "mountain.test.noao.edu" ]; then
#!    dqsvcpop  --loglevel DEBUG --queue transfer > $LOGDIR/dqsvcpop.log  2>&1 &
#!    dqsvcpush --loglevel DEBUG --queue transfer > $LOGDIR/dqsvcpush.log 2>&1 &
#!else
#!    dqsvcpop  --loglevel DEBUG --queue submit > $LOGDIR/dqsvcpop.log   2>&1 &
#!    dqsvcpush --loglevel DEBUG --queue submit > $LOGDIR/dqsvcpush.log  2>&1 &
#!fi

poplog=$LOGDIR/dqsvcpop.log
pushlog=$LOGDIR/dqsvcpush.log

if [ "`hostname`"  = "mountain.test.noao.edu" ]; then
    /usr/bin/dqsvcpop  --queue transfer --loglevel DEBUG > $poplog  2>&1 &
    /usr/bin/dqsvcpush --queue transfer --loglevel DEBUG > $pushlog 2>&1 &
else
    # Valley
    /usr/bin/dqsvcpop  --queue submit --loglevel DEBUG > $poplog   2>&1 &
    /usr/bin/dqsvcpush --queue submit   2>&1 &
fi


killdq ()
{
    echo "Killing DQ Push and Pop services"
    sudo kill -s SIGTERM `cat /var/run/dataq/*`
}

# tail -f $POPLOG   &

echo "To kill started the DQ Push and Pop services, execute: 'killdq'"
