# SOURCE this to save the PIDs

if [ "`hostname`"  = "mountain.test.noao.edu" ]; then
    dqsvcpop  --loglevel DEBUG --queue transfer &
    #!POPPID=$!
    dqsvcpush --loglevel DEBUG --queue transfer &
    #!PUSHPID=$!
else
    dqsvcpop  --loglevel DEBUG --queue submit &
    dqsvcpush --loglevel DEBUG --queue submit &
fi


jobs


killdq ()
{
    echo "Killing DQ Push and Pop services"
    #! kill -s SIGTERM $PUSHPID $POPPID
    kill -s SIGTERM `cat /var/run/dataq/*`
    sleep 1
    jobs
}


echo "To kill started the DQ Push and Pop services, execute: 'killdq'"
