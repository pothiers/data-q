DQ_CONFIG = {
    "queues": [
        {
            "name": "Generic-Data-Queue", #!!!
            "description": "This is meant as an example only.",
            "dq_host": "localhost",  # of data queue server (not redis)
            "dq_port": 9988,         # of data queue server (not redis)
            "action_name": "network_move",
            "maximum_errors_per_record": 0,
            "maxium_queue_size": 11000
        },
    ]
}

