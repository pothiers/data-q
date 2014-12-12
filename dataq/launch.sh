# SOURCE this 

export LOGDIR=/var/log/tada


if [ "`hostname`"  = "mountain.test.noao.edu" ]; then
    dqsvcpop  --loglevel DEBUG --queue transfer > $LOGDIR/dqsvcpop.log  2>&1 &
    dqsvcpush --loglevel DEBUG --queue transfer > $LOGDIR/dqsvcpush.log 2>&1 &
else
    dqsvcpop  --loglevel DEBUG --queue submit > $LOGDIR/dqsvcpop.log   2>&1 &
    dqsvcpush --loglevel DEBUG --queue submit > $LOGDIR/dqsvcpush.log  2>&1 &
fi

jobs


killdq ()
{
    echo "Killing DQ Push and Pop services"
    kill -s SIGTERM `cat /var/run/dataq/*`
}

# tail -f $POPLOG   &

echo "To kill started the DQ Push and Pop services, execute: 'killdq'"
