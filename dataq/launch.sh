# SOURCE this to save the PIDs

if [ "`hostname`"  = "mountain.test.noao.edu" ]; then
    QNAME="transfer"
else
    QNAME="submit"
fi


dqsvcpop  --loglevel DEBUG --queue $QNAME &
POPPID=$!
dqsvcpush --loglevel DEBUG --queue $QNAME &
PUSHPID=$!
jobs
echo "push PID: " $PUSHPID
echo "pop  PID: " $POPPID

killdq ()
{
    echo "Killing DQ Push and Pop services"
    kill -s SIGTERM $PUSHPID $POPPID
    jobs
}


echo "To kill started the DQ Push and Pop services, execute: 'killdq'"
