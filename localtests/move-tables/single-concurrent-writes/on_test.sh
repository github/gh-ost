#!/bin/bash

# insert data into the source primary, starting at ID 100 in batches of 10. kill
# the process after 5 seconds
DATABASE=test script/move-tables/insert-source-primary-loop 100 0.1 10 &
sleep 5 && kill $!
