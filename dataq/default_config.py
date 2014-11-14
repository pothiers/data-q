DQ_CONFIG = {
    "queues": [
        {
            "name": "Generic-Data-Queue",
            "description": "This is meant as an example only.",
            "host": "localhost",  # of queue server
            "port": 9988,         # of queue server
            "action_name": "echo00",
            "maximum_errors_per_record": 0,
            "maxium_queue_size": 11000
        },
    ]
}

