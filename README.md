# Instructions to Run

### Start Server

```sh
$ ./setup
$ source venv/bin/activate
$ cd src/
$ python3 setup.py develop
$ cd ..
$ python3 start_server.py 9996
```

### Run consumers

Open three different terminal windows and activate virtualenv using 
```sh
$ source venv/bin/activate
```
This is important because the code is installed as a package in the virtualenv and imported wherever necessary.
In each shell run:
```sh
$ python3 src/msgq/consumers/consumer_1.py
```
```sh
$ python3 src/msgq/consumers/consumer_2.py
```
```sh
$ python3 src/msgq/consumers/consumer_3.py
```

### To produce messages:

We can run this as many times as we want as it generates random message everytime.
```sh
$ python3 src/msgq/producer/producer_1.py
```