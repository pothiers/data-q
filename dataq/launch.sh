# SOURCE this 

export POPLOG=/var/log/dataq/dqsvcpop.log
export PUSHLOG=/var/log/dataq/dqsvcpush.log

if [ "`hostname`"  = "mountain.test.noao.edu" ]; then
    dqsvcpop  --loglevel DEBUG --queue transfer > $POPLOG  2>&1 &
    dqsvcpush --loglevel DEBUG --queue transfer > $PUSHLOG 2>&1 &
else
    dqsvcpop  --loglevel DEBUG --queue submit > $POPLOG  2>&1 &
    dqsvcpush --loglevel DEBUG --queue submit > $PUSHLOG 2>&1 &
fi

jobs


killdq ()
{
    echo "Killing DQ Push and Pop services"
    kill -s SIGTERM `cat /var/run/dataq/*`
}

# tail -f $POPLOG   &

echo "To kill started the DQ Push and Pop services, execute: 'killdq'"
