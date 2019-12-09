# RepCRec - Advanced Database Systems Project
Replicated Concurrency Control and Recovery (RepCRec)

### Team members:
- Zheng Jiang, Net ID: zj688
- Shih-Yao Chou, Net ID: syc574

## How to run
Please make sure you have Python 3.6+ installed.
To run the code directly, use the following command:
```bash
$ python3 main.py [input_file]
```

- If `input_file` is provided, the program will read instructions from 
the file line by line.
    - Lines that start with `//` are treated as comments and ignored.
    - The first line that starts with `===` is treated as a flag for 
      "debug info below". This line and all lines below it are ignored.
- If `input_file` is not provided, the program will read input from 
the standard input.
    - To exit the program, enter `exit`.
- Output always goes to the standard output.

## Use reprounzip
You can also use _reprounzip_ to unpack and run the `repcrec.rpz` package,
which includes 5 test cases.

#### Install reprounzip on CIMS server
To use _reprounzip_ on CIMS servers, first upload the `repcrec.rpz` file to
your server, using the following command:
```bash
$ scp /filepath/repcrec.rpz userid@access.cims.nyu.edu:/home/userid/desiredFolder
```

Then, perform the following steps on your CIMS server to satisfy prerequisites
for _reprounzip_:
```bash
$ module load python-2.7
$ python -c 'import reprozip'
$ echo $?
0
```
Getting 0 in the last step means success.

Then, install _reprounzip_:
```bash
$ pip install reprounzip
```

#### Unzip and run
If you wish to check the files in the package and their info, use the following
commands:
```bash
$ reprounzip showfiles repcrec.rpz
$ reprounzip info repcrec.rpz
```

If it all looks fine then unzip it to the desired location
(e.g. `unzip` folder in the current directory):
```bash
$ reprounzip directory setup repcrec.rpz ./unzip
```

Finally, run the directory using:
```bash
$ reprounzip directory run ./unzip
```

It should run through the packed test cases and you can check the output.
