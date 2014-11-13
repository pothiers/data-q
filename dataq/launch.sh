# SOURCE this to save the PIDs

dqsvcpop  --loglevel DEBUG &
POPPID=$!
dqsvcpush --loglevel DEBUG &
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
